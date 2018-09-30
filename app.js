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

const getRelatedKeywords = async ({service, keyword, searchUrl, paramName, selector, proxy}) => {
    try {
        const response = await axios.get(searchUrl, {
            params: {[paramName]: keyword},
            headers: {
                'user-agent' : 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:46.0) Gecko/20100101 Firefox/46.0'
            },
            timeout: 3000,
            proxy: proxy
        });

        const $html = $(response.data);
        const $links = $html.find(selector);
        // const $links = $html.find(".sp_keyword .lst_relate li a");

        return Array.from($links.map((i, link) => {
            return deleteKeyword($(link).text(), keyword);
        }));
    }catch(e){
        console.error(e.stack);
        console.trace("from");
        console.log(e.response && e.response.data);
        return [];
    }
};

const crawler = async ({service, keyword, searchUrl, paramName, selector, delayTime, table}) => {
    let visited = {};
    let queue = [keyword];

    while(queue.length > 0){
        const currentKeyword = queue.shift();
        const linkedKeywords = await getRelatedKeywords({keyword: currentKeyword, service, searchUrl, paramName, selector});

        console.log(`${Object.keys(visited).length}\t${currentKeyword}\t= ${linkedKeywords.join()}`);

        const nextDestination = linkedKeywords.filter(linkedKeyword => !visited[linkedKeyword]);

        queue = queue.concat(nextDestination);
        visited = nextDestination.reduce((visited, keyword) => ({...visited, ...{
            [keyword]: (visited[keyword] || 0) + 1
        }}), visited);

        if(linkedKeywords.length > 0){
            await table.insert(linkedKeywords.map(linkedKeyword => ({
                service: service,
                from: currentKeyword,
                to: linkedKeyword,
                timestamp: bigquery.datetime(new Date().toISOString())
            })));
        }

        await delay(delayTime);
    }
};

(async () => {
    console.log('setting up table...');

    const table = await setupTable({
        datasetName: 'search_keyword_study_data',
        tableName: 'search_keyword_graph'
    });

    console.log('start crawling');
        await crawler({
        service: 'google',
        keyword: '아이유',
        searchUrl: 'https://www.google.co.kr/search',
        paramName: "q",
        selector : "#botstuff .card-section a",
        delayTime: 100,
        table: table
    });

    console.log('finished');
})();



