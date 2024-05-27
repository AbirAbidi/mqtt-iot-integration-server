const express = require('express');
const { MongoClient } = require('mongodb');
const mqtt = require('mqtt');

const app = express();
const PORT = 1883;
const HOST = '0.0.0.0';
const MONGODB_URL = 'mongodb://127.0.0.1:27017';
const DB_NAME = 'PlateformIOTdb';
const COLLECTION_NAME = 'sensors';
const MQTT_BROKER_URL = 'mqtt://91.121.93.94'; // Change this to your MQTT broker IP

// Middleware to parse JSON bodies
app.use(express.json());

// MongoDB client instance
const client = new MongoClient(MONGODB_URL, { useNewUrlParser: true, useUnifiedTopology: true });

// Connect to MongoDB
async function connectToDB() {
    try {
        await client.connect();
        console.log('Connected to MongoDB');
    } catch (error) {
        console.error('Error connecting to MongoDB:', error);
        process.exit(1); // Exit the process if unable to connect to MongoDB
    }
}
connectToDB();

// MQTT client instance
const mqttClient = mqtt.connect(MQTT_BROKER_URL);

// Handle MQTT messages
mqttClient.on('message', async (topic, message) => {
    try {
        // Parse the message
        const dataParts = message.toString().split(',');

        // Initialize variables to store uniqueId and value
        let uniqueId = null;
        let value = null;

        // Iterate over data parts to extract uniqueId and value
        dataParts.forEach(part => {
            const [key, val] = part.split(':');
            if (key === 'uniqueId') {
                uniqueId = val.trim();
            } else if (key === 'value') {
                value = parseFloat(val.trim());
            }
        });

        // Check if uniqueId and value are present
        if (uniqueId !== null && value !== null) {
            // Insert the data into MongoDB or perform any other necessary operations
            const db = client.db(DB_NAME);
            const collection = db.collection(COLLECTION_NAME);

            const currentTime = new Date();
            const newValue = {
                value: value,
                time: currentTime
            };

            // Find document in the collection where uniqueId matches
            const query = { uniqueId: uniqueId };
            const existingDocument = await collection.findOne(query);

            if (existingDocument) {
                // Document with matching uniqueID found, append the new value to the values array
                const updateValues = {
                    $push: { values: newValue } // Append value with timestamp to the values array
                };
                const result = await collection.updateOne(query, updateValues);
                console.log('Document updated successfully:', result.modifiedCount);
            } else {
                // No document found with matching uniqueID, print error message
                console.log('Sensor with unique ID', uniqueId, 'does not exist');
            }
        } else {
            console.error('Missing uniqueId or value in message');
        }
    } catch (error) {
        console.error('Error handling MQTT message:', error);
    }
});

// Subscribe to MQTT topic
mqttClient.on('connect', () => {
    mqttClient.subscribe('data', (err) => {
        if (err) {
            console.error('Error subscribing to MQTT topic:', err);
        } else {
            console.log('Subscribed to MQTT topic: data');
        }
    });
});

// Handle POST request to '/api/post_data'
app.post('/api/post_data', async (req, res) => {
    try {
        const data = req.body;
        if (!data) {
            return res.status(400).json({ error: 'No data provided' });
        }

        const db = client.db(DB_NAME);
        const collection = db.collection(COLLECTION_NAME);

        const result = await collection.insertOne(data);
        console.log('Data inserted successfully:', result.insertedId);
        res.status(201).json({ message: 'Data inserted successfully', insertedId: result.insertedId });
    } catch (error) {
        console.error('Error inserting data:', error);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// Start the server
app.listen(PORT, HOST, () => {
    console.log(`Server is running on http://${HOST}:${PORT}`);
});
