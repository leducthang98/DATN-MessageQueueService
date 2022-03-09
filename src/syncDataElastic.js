const mysql = require('mysql')
const elasticUtil = require('./elasticSearch')
const pool = mysql.createPool('mysql://root:codedidungso.me@localhost:3306/yuubook');

const query = async (sql, params) => {
    console.log('----------------------------');
    console.log('sql:', mysql.format(sql, params));
    console.log('----------------------------');
    return new Promise((resolve, reject) => {
        pool.query(sql, params, (error, results) => {
            if (error) {
                return reject(error);
            }
            return resolve(results);
        });
    });
};

async function run() {
    const data = await query('select b.*, a.name as authorName from book b left join author a on b.author_id = a.id where b.status = 1')
    let dataFormatted = []
    for (const e of data) {
        dataFormatted.push({
            id: e.id,
            name: e.name,
            avatar: e.avatar,
            authorName: e.authorName,
            status: e.status,
            slug: e.slug
        })
    }
    for (const e of dataFormatted) {
        await elasticUtil.upsertBook('book', e.id, e)
    }
}

run()