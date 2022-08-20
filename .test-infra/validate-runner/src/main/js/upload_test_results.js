import { Storage } from '@google-cloud/storage';

const up = () => {
  const gcs = new Storage({
    projectId: 'apache-beam-testing',
    keyFilename: 'website/www/site/data/latest_capability_matrix.json',
  });
  const bucket = gcs.bucket('beam-validates-runner-info');
  bucket.upload(
    './website/www/site/data/latest_capability_matrix.json',
    function (err, file) {
      if (err) console.log('Error no input file');
      //   throw new Error('No file presented');
    }
  );
};
up();
