import express from 'express';
import bodyParser from 'body-parser';
import cors from 'cors';
import { publishMessage } from './kafka/producer';
import { bookIndex, createDocument, searchBook, upsertBook } from './elasticSearch';
require('./kafka/consumer')
const expressApp = express();

const corsOptions = {
    origin: '*',
    'Access-Control-Expose-Headers': 'Content-Range',
    methods: 'GET,HEAD,PUT,PATCH,POST,DELETE',
    optionsSuccessStatus: 200
};

const API_KEY = 'bGVkdWN0aGFuZzk4QGdtYWlsLmNvbQ=='

expressApp.use(cors(corsOptions));

expressApp.use(bodyParser.json({
    limit: '50mb'
}));
expressApp.use(bodyParser.urlencoded({
    extended: true,
    limit: '50mb',
}));

expressApp.get('/health_check', (req, res) => {
    res.send('ok')
})

expressApp.post('/kafka/push', async (req, res) => {
    if (req.headers.api_key === API_KEY) {
        const messagePayload = req.body
        await publishMessage(messagePayload)
        res.send('ok')
    } else {
        res.status(403).send({
            status: 'permission denied'
        })
    }
})

expressApp.get('/searchElastic/book', async (req, res) => {
    if (req.headers.api_key === API_KEY) {
        const { searchKey } = req.query
        const dataSearch = await searchBook(bookIndex, searchKey)
        let hitRecords = dataSearch?.hits?.hits
        let resp = []
        for (const hitRecord of hitRecords ){
            resp.push(hitRecord._source)
        }
        res.send(resp)
    } else {
        res.status(403).send({
            status: 'permission denied'
        })
    }
})

expressApp.listen(5000, async () => {
    console.log('Server is running at port', 5000)
});

