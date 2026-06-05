require('dotenv').config();
const express = require('express');
const cors = require('cors');
const mongoose = require('mongoose');
const QRCode = require('qrcode');
const axios = require('axios');
const cloudinary = require('cloudinary').v2;
const makeWASocket = require('@whiskeysockets/baileys').default;
const { DisconnectReason, initAuthCreds, proto, downloadMediaMessage } = require('@whiskeysockets/baileys');

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3001;
const MONGODB_URI = process.env.MONGODB_URI;
const QR_SERVICE_SECRET = process.env.QR_SERVICE_SECRET;

// Configure Cloudinary
cloudinary.config({
    cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
    api_key: process.env.CLOUDINARY_API_KEY,
    api_secret: process.env.CLOUDINARY_API_SECRET
});

// Connect to MongoDB
mongoose.connect(MONGODB_URI)
    .then(() => {
        console.log("DB connected successfully for QR service");
        initAllSessions();
    })
    .catch((err) => console.error("Database connection error:", err));

// Database Schemas
const BaileysAuthSchema = new mongoose.Schema({
    userId: { type: String, required: true, index: true },
    key: { type: String, required: true },
    value: { type: String, required: true }
});
BaileysAuthSchema.index({ userId: 1, key: 1 }, { unique: true });
const BaileysAuthModel = mongoose.models.BaileysAuth || mongoose.model("BaileysAuth", BaileysAuthSchema);

const UserSchema = new mongoose.Schema({
    whatsapp: {
        connected: Boolean,
        connectionType: String,
        accessToken: String,
        wabaId: String,
        phoneNumberId: String,
        displayPhone: String,
        connectedAt: Date,
        qr: {
            sessionId: String,
            sessionData: String,
            lastSeen: Date,
            qrCode: String,
            qrExpiresAt: Date
        }
    },
    whatsappBusinessNo: String
}, { strict: false });
const UserModel = mongoose.models.User || mongoose.model("User", UserSchema);

// Serializers for Buffers in MongoDB
const replacer = (key, value) => {
    if (value && value.type === 'Buffer' && Array.isArray(value.data)) {
        return { type: 'Buffer', data: Buffer.from(value.data).toString('base64') };
    }
    return value;
};

const reviver = (key, value) => {
    if (value && value.type === 'Buffer' && typeof value.data === 'string') {
        return Buffer.from(value.data, 'base64');
    }
    return value;
};

// Custom MongoDB Auth State Store
async function useMongoAuthState(userId) {
    const writeData = async (data, key) => {
        const value = JSON.stringify(data, replacer);
        await BaileysAuthModel.findOneAndUpdate(
            { userId, key },
            { value },
            { upsert: true, new: true }
        );
    };

    const readData = async (key) => {
        try {
            const res = await BaileysAuthModel.findOne({ userId, key });
            if (!res) return null;
            return JSON.parse(res.value, reviver);
        } catch (err) {
            return null;
        }
    };

    const removeData = async (key) => {
        await BaileysAuthModel.deleteOne({ userId, key });
    };

    let creds = await readData('creds');
    if (!creds) {
        creds = initAuthCreds();
        await writeData(creds, 'creds');
    }

    return {
        state: {
            creds,
            keys: {
                get: async (type, ids) => {
                    const data = {};
                    await Promise.all(
                        ids.map(async (id) => {
                            let value = await readData(`${type}-${id}`);
                            if (type === 'app-state-sync-key' && value) {
                                value = proto.Message.AppStateSyncKeyData.fromObject(value);
                            }
                            data[id] = value;
                        })
                    );
                    return data;
                },
                set: async (data) => {
                    const tasks = [];
                    for (const category in data) {
                        for (const id in data[category]) {
                            const value = data[category][id];
                            const key = `${category}-${id}`;
                            if (value) {
                                tasks.push(writeData(value, key));
                            } else {
                                tasks.push(removeData(key));
                            }
                        }
                    }
                    await Promise.all(tasks);
                }
            }
        },
        saveCreds: async () => {
            await writeData(creds, 'creds');
        },
        clearAuth: async () => {
            await BaileysAuthModel.deleteMany({ userId });
        }
    };
}

// Active Sessions Cache
const activeSessions = new Map();

// Helper to upload media to Cloudinary
const uploadBufferToCloudinary = (buffer, userId, customerPhone) => {
    return new Promise((resolve, reject) => {
        cloudinary.uploader.upload_stream(
            {
                folder: `pax26/${userId}/customer-images`,
                tags: [`seller-${userId}`, `customer-${customerPhone}`, `customer-image`],
                resource_type: "image",
                visual_search: true
            },
            (error, result) => {
                if (error) reject(error);
                else resolve(result);
            }
        ).end(buffer);
    });
};

// Helper to post webhook payloads to Next.js
const postWebhookToNext = async (payload) => {
    try {
        const url = `${process.env.NEXT_APP_URL}/api/webhooks/qr`;
        await axios.post(url, payload, {
            headers: {
                'Authorization': `Bearer ${QR_SERVICE_SECRET}`,
                'Content-Type': 'application/json'
            }
        });
        console.log("Successfully posted message to Next.js webhook");
    } catch (err) {
        console.error("Failed to post message to Next.js webhook:", err.response?.data || err.message);
    }
};

// Reconnect/Start WhatsApp Connection
async function startSession(userId) {
    if (activeSessions.has(userId)) {
        const existing = activeSessions.get(userId);
        if (existing.status === 'CONNECTED') {
            return existing;
        }
        await stopSession(userId);
    }

    console.log(`[Session ${userId}] Initializing...`);
    const { state, saveCreds, clearAuth } = await useMongoAuthState(userId);

    const sock = makeWASocket({
        auth: state,
        printQRInTerminal: false
    });

    const sessionInfo = {
        sock,
        qr: null,
        status: 'INITIALIZING',
        connectedPhone: null
    };
    activeSessions.set(userId, sessionInfo);

    sock.ev.on('creds.update', saveCreds);

    sock.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect, qr } = update;

        if (qr) {
            console.log(`[Session ${userId}] QR Code generated`);
            try {
                const qrImageBase64 = await QRCode.toDataURL(qr);
                sessionInfo.qr = qrImageBase64;
                sessionInfo.status = 'QR';

                await UserModel.findByIdAndUpdate(userId, {
                    $set: {
                        "whatsapp.qr.sessionId": userId,
                        "whatsapp.qr.qrCode": qrImageBase64,
                        "whatsapp.qr.qrExpiresAt": new Date(Date.now() + 40000),
                        "whatsapp.connected": false,
                        "whatsapp.connectionType": "qr"
                    }
                });
            } catch (err) {
                console.error(`Failed to generate QR for user ${userId}:`, err);
            }
        }

        if (connection === 'open') {
            const cleanPhone = `+${sock.user.id.split(':')[0]}`;
            console.log(`[Session ${userId}] WhatsApp Connected: ${cleanPhone}`);
            sessionInfo.status = 'CONNECTED';
            sessionInfo.qr = null;
            sessionInfo.connectedPhone = cleanPhone;

            await UserModel.findByIdAndUpdate(userId, {
                $set: {
                    "whatsapp.connected": true,
                    "whatsapp.connectionType": "qr",
                    "whatsapp.phoneNumberId": cleanPhone,
                    "whatsapp.displayPhone": cleanPhone,
                    "whatsapp.qr.qrCode": null,
                    "whatsapp.qr.qrExpiresAt": null,
                    "whatsapp.qr.sessionId": userId,
                    "whatsapp.connectedAt": new Date(),
                    "whatsappBusinessNo": cleanPhone
                }
            });
        }

        if (connection === 'close') {
            const statusCode = lastDisconnect?.error?.output?.statusCode;
            const isLoggedOut = statusCode === DisconnectReason.loggedOut;
            // Code 515 = stream error / server-side forced disconnect — wipe creds and start fresh
            const isStreamError = statusCode === 515;
            const shouldReconnect = !isLoggedOut;

            console.log(`[Session ${userId}] Connection closed. StatusCode: ${statusCode} Reconnect: ${shouldReconnect}`);

            sessionInfo.status = 'DISCONNECTED';
            sessionInfo.qr = null;
            // Remove from active map immediately so stopSession won't try to logout again
            activeSessions.delete(userId);

            if (isLoggedOut || isStreamError) {
                console.log(`[Session ${userId}] ${isStreamError ? 'Stream error 515' : 'Logged out'}. Wiping credentials.`);
                try { await clearAuth(); } catch (e) {}

                await UserModel.findByIdAndUpdate(userId, {
                    $set: {
                        "whatsapp.connected": false,
                        "whatsapp.qr.qrCode": null,
                        "whatsapp.qr.qrExpiresAt": null,
                        "whatsapp.qr.sessionId": null,
                        "whatsapp.phoneNumberId": "",
                        "whatsapp.displayPhone": ""
                    }
                });

                if (isStreamError) {
                    // After 515, wait longer before retrying — WhatsApp needs a moment
                    console.log(`[Session ${userId}] Will retry fresh session in 8s...`);
                    setTimeout(() => startSession(userId), 8000);
                }
            } else {
                setTimeout(() => startSession(userId), 5000);
            }
        }
    });

    sock.ev.on('messages.upsert', async (upsert) => {
        if (upsert.type !== 'notify') return;

        for (const msg of upsert.messages) {
            if (!msg.message) continue;
            if (msg.key.fromMe) continue;

            const fromJid = msg.key.remoteJid;
            if (fromJid.endsWith('@g.us')) continue; // DMs only
            if (!fromJid.endsWith('@s.whatsapp.net')) continue;

            const cleaned = fromJid.split('@')[0];
            const visitorPhone = `+${cleaned}`;

            let text = '';
            let mediaUrl = null;
            let mediaType = null;
            let caption = '';

            const messageType = Object.keys(msg.message)[0];
            if (messageType === 'conversation') {
                text = msg.message.conversation;
            } else if (messageType === 'extendedTextMessage') {
                text = msg.message.extendedTextMessage.text;
            } else if (messageType === 'imageMessage') {
                caption = msg.message.imageMessage.caption || '';
                text = caption || '📷 Image';
                mediaType = 'image';

                try {
                    const buffer = await downloadMediaMessage(msg, 'buffer', {});
                    const uploadResult = await uploadBufferToCloudinary(buffer, userId, cleaned);
                    mediaUrl = uploadResult.secure_url;
                } catch (err) {
                    console.error(`[Session ${userId}] Failed to process image:`, err);
                }
            } else {
                continue;
            }

            console.log(`[Session ${userId}] Incoming DM from ${visitorPhone}: ${text}`);

            await postWebhookToNext({
                userId,
                entry: [{
                    changes: [{
                        value: {
                            metadata: {
                                phone_number_id: sessionInfo.connectedPhone || "",
                                display_phone_number: sessionInfo.connectedPhone || ""
                            },
                            messages: [{
                                from: visitorPhone.replace('+', ''),
                                id: msg.key.id,
                                type: mediaType || 'text',
                                text: { body: text },
                                ...(mediaUrl && { image: { url: mediaUrl, caption } })
                            }]
                        }
                    }]
                }]
            });
        }
    });

    return sessionInfo;
}

// Stop WhatsApp Session
async function stopSession(userId) {
    if (activeSessions.has(userId)) {
        const { sock } = activeSessions.get(userId);
        // Safely close — socket may already be closed (e.g. after stream error 515)
        try { sock.end(undefined); } catch (e) {}
        activeSessions.delete(userId);
    }
    try {
        const { clearAuth } = await useMongoAuthState(userId);
        await clearAuth();
    } catch (e) {
        console.error(`[Session ${userId}] clearAuth error (non-fatal):`, e.message);
    }
}

// Initialize active sessions from DB
async function initAllSessions() {
    try {
        const users = await UserModel.find({
            "whatsapp.connected": true,
            "whatsapp.connectionType": "qr"
        });
        console.log(`Restoring ${users.length} active QR WhatsApp sessions...`);
        for (const user of users) {
            startSession(user._id.toString()).catch((err) => {
                console.error(`Error restoring session for user ${user._id}:`, err);
            });
        }
    } catch (err) {
        console.error("Error restoring sessions:", err);
    }
}

// Middleware: Authenticate requests using QR_SERVICE_SECRET
function authMiddleware(req, res, next) {
    const authHeader = req.headers['authorization'];
    const token = authHeader && authHeader.split(' ')[1];
    if (!token || token !== QR_SERVICE_SECRET) {
        return res.status(401).json({ error: 'Unauthorized' });
    }
    next();
}

// Endpoints
app.post('/api/session/start', authMiddleware, async (req, res) => {
    const { userId } = req.body;
    if (!userId) return res.status(400).json({ error: 'Missing userId' });

    try {
        const session = await startSession(userId);
        res.json({ success: true, status: session.status, qr: session.qr });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.get('/api/session/status', authMiddleware, async (req, res) => {
    const { userId } = req.query;
    if (!userId) return res.status(400).json({ error: 'Missing userId' });

    const session = activeSessions.get(userId);
    if (!session) {
        return res.json({ success: true, status: 'DISCONNECTED', qr: null });
    }
    res.json({ success: true, status: session.status, qr: session.qr });
});

app.post('/api/session/stop', authMiddleware, async (req, res) => {
    const { userId } = req.body;
    if (!userId) return res.status(400).json({ error: 'Missing userId' });

    try {
        await stopSession(userId);
        res.json({ success: true, message: 'Session stopped successfully' });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.post('/api/message/send', authMiddleware, async (req, res) => {
    const { userId, to, text, imageUrl, caption } = req.body;
    if (!userId || !to) return res.status(400).json({ error: 'Missing parameters' });

    const session = activeSessions.get(userId);
    if (!session || session.status !== 'CONNECTED') {
        return res.status(400).json({ error: 'WhatsApp session not connected' });
    }

    try {
        const jid = `${to.replace('+', '')}@s.whatsapp.net`;
        let result;

        if (imageUrl) {
            result = await session.sock.sendMessage(jid, {
                image: { url: imageUrl },
                caption: caption || ""
            });
        } else {
            result = await session.sock.sendMessage(jid, {
                text: text
            });
        }

        res.json({ success: true, messageId: result.key.id });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.listen(PORT, () => {
    console.log(`QR service listening on port ${PORT}`);
});
