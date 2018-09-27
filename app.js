import BigQuery from '@google-cloud/bigquery'

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
            schema: 'name:string, value:integer'
        });
    }
    return table;
}

(async () => {
    const table = await setupTable({
        datasetName: 'search_keyword_study_data',
        tableName: 'search_keyword_graph'
    });
    table.insert({
        name: 'Motion Picture Institute of Michigan',
        value: 1
    })
})();