import { kafkaClient } from "."
import { ELASTIC_TOPIC } from "./constant"
import { v4 as uuidv4 } from 'uuid';
import { bookIndex, deleteBookById, upsertBook } from "../elasticSearch";

async function run() {
    const consumer = kafkaClient.consumer({ groupId: uuidv4() })
    await consumer.connect()
    await consumer.subscribe({ topic: ELASTIC_TOPIC, fromBeginning: false })
    await consumer.run({
        autoCommit: true,
        autoCommitMsgCount: 100,
        autoCommitIntervalMs: 5000,
        fetchMaxWaitMs: 100,
        fetchMinBytes: 1,
        fetchMaxBytes: 1024 * 10,
        fromOffset: true,
        encoding: 'utf8',
        partitionsConsumedConcurrently: 2,
        eachMessage: async ({ topic, partition, message }) => {
            await processMessage(JSON.parse(message.value.toString()))
            return
        },
    })
}

async function processMessage(message) {
    console.log('message:', message)
    const { type, action, data } = message
    if (type === 'book') {
        switch (action) {
            case 'create':
                if (data.id) {
                    await upsertBook(bookIndex, data.id, data)
                }
                break
            case 'update':
                if (data.id) {
                    await upsertBook(bookIndex, data.id, data)
                }
                break
            case 'delete':
                if (data.id) {
                    await deleteBookById(bookIndex, data.id)
                }
                break
        }
    } else if (type === 'author') {
        switch (action) {
            case 'update':
                // TODO: ignore this case
                break
            case 'delete':
               // TODO: ignore this case
                break
        }
    }
}

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.forEach(type => {
    process.on(type, async e => {
        try {
            console.log(`process.on ${type}`)
            console.error(e)
            await consumer.disconnect()
            process.exit(0)
        } catch (_) {
            process.exit(1)
        }
    })
})

signalTraps.forEach(type => {
    process.once(type, async () => {
        try {
            console.log('consumer disconnected')
            await consumer.disconnect()
        } finally {
            process.kill(process.pid, type)
        }
    })
})

run()