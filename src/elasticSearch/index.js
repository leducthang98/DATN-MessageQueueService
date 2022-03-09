import axios from "axios"

const ELASTIC_SEARCH_URL = 'http://localhost:9200'
export const bookIndex = 'book'

export const upsertBook = async (index, id, document) => {
    console.log('===UPSERT_BOOK===')
    console.log(index, id, document)
    document.status = parseInt(document.status)
    let res = null
    try {
        res = await axios.put(`${ELASTIC_SEARCH_URL}/${index || bookIndex}/doc/${id}`, document)
        console.log('upsert book success:', document)
    } catch (error) {
        console.error('upsert book fail:', error.response)
    }
    return res
}

export const deleteBookById = async (index, id) => {
    let res = null
    try {
        res = await axios.delete(`${ELASTIC_SEARCH_URL}/${index || bookIndex}/doc/${id}`)
        console.log('delete book success:', id)
    } catch (error) {
        console.error('delete book fail:', error)
    }
    return res
}

export const searchBook = async (index, searchKey = "") => {
    var data = JSON.stringify({ "query": { "multi_match": { "query": searchKey, "fields": ["name", "authorName"], "fuzziness": 2 } } });
    var config = {
        method: 'get',
        url: `${ELASTIC_SEARCH_URL}/${index || bookIndex}/_search?size=10&pretty`,
        headers: {
            'Content-Type': 'application/json'
        },
        data: JSON.parse(data)
    };
    let res = null
    try {
        let respData = await axios(config)

        res = respData.data
    } catch (error) {
        console.log(error)
    }
    return res
}