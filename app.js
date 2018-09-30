import BigQuery from '@google-cloud/bigquery'
import axios from 'axios'
import $ from 'cheerio'

const bigquery = new BigQuery({projectId: 'searchkeywordstudy'});
const getFirst = data => data[0];

const delay = milliseconds => new Promise(resolve => setTimeout(() => resolve(), milliseconds));

const deleteKeyword = (t, keyword) => t.replace(new RegExp(`(^| )${keyword}($| )`,"g")," ").trim();

const setupTable = async ({datasetName, tableName}) => {
    const dataset = await bigquery.dataset(datasetName).get({autoCreate: true}).then(getFirst);
    const table = await dataset.table(tableName).get({autoCreate: true}).then(getFirst);
    const metaData = await table.getMetadata().then(getFirst);

    if (!metaData.schema) {
        await table.setMetadata({
            name: 'Search Keyword Graph',
            description: 'A table for storing Search Keyword Graph.',
            schema: 'service:string, from:string, to:string, timestamp:timestamp'
        });
    }
    return table;
};

const getRelatedKeywords = async ({service, keyword, searchUrl, paramName, proxy}) => {
    try {
        const response = await axios.get(searchUrl, {
            params: {[paramName]: keyword},
            headers: {'user-agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36'},
            timeout: 3000,
            proxy: proxy
        });

        const $html = $(response.data);
        const $links = $html.find(".sp_keyword .lst_relate li a");

        return Array.from($links.map((i, link) => {
            return deleteKeyword($(link).text(), keyword);
        }));
    }catch(e){
        console.log(e);
        return [];
    }
};

const crawler = async ({service, keyword, searchUrl, paramName, delayTime, table}) => {
    let visited = {};
    let queue = [keyword];

    while(queue.length > 0){
        const currentKeyword = queue.shift();
        const linkedKeywords = await getRelatedKeywords({
            keyword: currentKeyword,
            service: service,
            searchUrl: searchUrl,
            paramName: paramName
        });

        const nextKeywords = linkedKeywords.filter(linkedKeyword => !visited[linkedKeyword]);

        console.log(currentKeyword, "=", nextKeywords.join());

        queue = queue.concat(nextKeywords);
        visited = nextKeywords.reduce((visited, keyword) => ({...visited, ...{
            [keyword]: (visited[keyword] || 0) + 1
        }}), visited);

        await table.insert(linkedKeywords.map(linkedKeyword => ({
            service: service,
            from: keyword,
            to: linkedKeyword,
            timestamp: bigquery.datetime(new Date().toISOString())
        })));
        await delay(delayTime);
    }
};

(async () => {
    console.log('start');

    await crawler({
        service: 'naver',
        keyword: '아이유',
        searchUrl: 'http://search.naver.com/search.naver',
        paramName: "query",
        delayTime: 2000,
        table: { insert() {} }
    });

    console.log('end');
})();



