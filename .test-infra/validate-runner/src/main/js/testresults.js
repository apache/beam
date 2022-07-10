// reformat results to json type
import fs from 'fs';
import data from './testresults.json';

const statusDescription = {
  yes: 'Yes',
  yesSupported: 'fully supported',
  partially: 'Partially',
  partiallySupported: 'fully supported in batch mode',
  no: 'No',
  notImplemented: 'not implemeted',
};

const validatesRunner = 'org.apache.beam.sdk.testing.ValidatesRunner';
let categs = {};
Object.keys(data.batch).forEach((key, i) => {
  let curr = [];
  let dir = {};
  let counts = 0;
  data.batch[key].testCases.forEach((test) => {
    if (test.categories.length === 0) return;
    test.categories
      .filter((item) => item !== validatesRunner)
      .forEach((cat, i) => {
        let passed = test.status === 'PASSED';
        if (categs[cat] === undefined) {
          let categoryName = cat.slice(cat.lastIndexOf('.') + 1);
          categs[cat] = {
            name: categoryName,
            values: [],
          };
        }
        if (dir[cat] === undefined) {
          dir[cat] = counts;
          counts++;
          curr.push({
            name: cat,
            values: [
              {
                class: key,
                [passed ? 'l3' : 'l4']: [
                  { name: test.name, status: test.status },
                ],
              },
            ],
          });
        } else {
          let temp = curr[dir[cat]].values[0];
          if (temp[passed ? 'l3' : 'l4'] === undefined)
            temp[passed ? 'l3' : 'l4'] = [
              {
                name: test.name,
                status: test.status,
              },
            ];
          else
            temp[passed ? 'l3' : 'l4'].push({
              name: test.name,
              status: test.status,
            });
          /*               temp[passed ? 'L3' : 'L4'].push({name:test.name, status:test.status})
           */
        }
      });
  });

  curr.forEach((result) => {
    let currentValues = result.values[0];
    if (result.values[0]['l4'] === undefined) {
      currentValues['l1'] = `${statusDescription.yes}`;
      currentValues['l2'] = `${statusDescription.yesSupported}`;
    } else if (result.values[0]['l3'].length > 0) {
      currentValues['l1'] = `${statusDescription.partially}`;
      currentValues['l2'] = `${statusDescription.partiallySupported}`;
    }
    categs[result.name].values.push(currentValues);
  });
});

const names = {
  flink: 'Apache Flink',
  dataflow: 'Google Cloud Dataflow',
  spark: 'Apache Spark (RDD/DStream based)',
};

let formattedData = {
  capability_matrix: {
    columns: [],
    categories: [
      {
        description: 'Capability Matrix (based on runner testing)',
        anchor: 'what',
        'color-y': 'fff',
        'color-yb': 'f6f6f6',
        'color-p': 'f9f9f9',
        'color-pb': 'd8d8d8',
        'color-n': 'e1e0e0',
        'color-nb': 'bcbcbc',
      },
    ],
  },
};

Object.keys(data.batch).forEach((key) => {
  formattedData.capability_matrix.columns.push({
    class: key,
    name: `${names[key]}`,
  });
});

formattedData.capability_matrix.categories[0]['rows'] = Object.keys(categs).map(
  (key) => categs[key]
);

let jsonFormatted = JSON.stringify(formattedData);

fs.writeFile(
  'website/www/site/data/myjsonfile.json',
  jsonFormatted,
  'utf8',
  () => {}
);

console.log('resultRows', jsonFormatted);
// console.dir(formattedData, { depth: null });
// console.log('data', formattedData)
