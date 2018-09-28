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

(async () => {

    const table = await setupTable({
        datasetName: 'search_keyword_study_data',
        tableName: 'search_keyword_graph'
    });

    const service = 'naver';
    const keyword = "아이유";
    const naverSearchUrl = 'https://search.naver.com/search.naver';
    const userAgentString = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36';
    const KeywordDeleteRegex = new RegExp(keyword,"g");

    try{
        const response = await axios.get(naverSearchUrl, {
            params: {
                query: keyword
            },
            headers: {
                'user-agent' : userAgentString
            },
            timeout: 3000
        });
        const $html = $(response.data);
        const $links = $html.find(".sp_keyword .lst_relate li a");

        const linkedKeywords = $links.map((i, link) => $(link).text().replace(KeywordDeleteRegex,"").trim());
        const insertList = Array.from(linkedKeywords)
            .map(linkedKeyword => ({
                service: service,
                from: keyword,
                to: linkedKeyword,
                timestamp: bigquery.datetime(new Date().toISOString())
            }));

        await table.insert(insertList);
    }catch(error){
        console.error(JSON.stringify(error));
    }
})();