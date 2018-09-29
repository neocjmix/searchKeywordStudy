import BigQuery from '@google-cloud/bigquery'
import axios from 'axios'
import $ from 'cheerio'

const bigquery = new BigQuery({projectId: 'searchkeywordstudy'});
const getFirst = data => data[0];

async function setupTable({datasetName, tableName}) {
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
}

function deleteKeyword(t, keyword){
    return t.replace(new RegExp(`(^| )${keyword}($| )`,"g")," ").trim()
}

async function getRelatedKeywords({service, keyword, searchUrl, paramName}){
    const userAgentString = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36';

    const response = await axios.get(searchUrl, {
        params: {
            [paramName]: keyword
        },
        headers: {
            'user-agent' : userAgentString
        },
        timeout: 3000
    });
    const $html = $(response.data);
    const $links = $html.find(".sp_keyword .lst_relate li a");

    return Array.from($links.map((i, link) => {
        return deleteKeyword($(link).text(), keyword);
    }));
}

function delay(milliseconds){
    return new Promise(resolve => setTimeout(() => resolve(), milliseconds));
}

const crawler = ({service, keyword, searchUrl, paramName, delayTime, table}) => {
    const visit = async (keyword, visited = {}, depth = 0) => {
        console.log(keyword, depth);
        const linkedKeywords = await getRelatedKeywords({
            keyword: keyword,
            service: service,
            searchUrl: searchUrl,
            paramName: paramName,
        });

        await table.insert(linkedKeywords.map(linkedKeyword => ({
            service: service,
            from: keyword,
            to: linkedKeyword,
            timestamp: bigquery.datetime(new Date().toISOString())
        })));

        await delay(delayTime);

        const visitedIncludingCurrent = linkedKeywords.reduce((visited, linkedKeyword) => ({...visited, ...{[linkedKeyword]: true}}), visited);
        const revisitFilteredKeywords = linkedKeywords.filter(linkedKeyword => !visited[linkedKeyword]);

        return await revisitFilteredKeywords
            .reduce(async (visitedPromise, linkedKeyword) => {
                const visited = await visitedPromise;
                return {...visited, ...await visit(linkedKeyword, visited, depth+1)};
            }, visitedIncludingCurrent);
    };

    return visit;
};

(async () => {
    console.log('start');

    await (crawler({
        service: 'naver',
        searchUrl: 'https://search.naver.com/search.naver',
        paramName: "query",
        delayTime: 10,
        table: { insert() {} }
    })('아이유'));

    console.log('end');
})();



