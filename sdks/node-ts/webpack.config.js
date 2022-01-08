const tsClosureTransform = require('ts-closure-transform');
const path = require('path');
module.exports = {
  module: {
    rules: [
      {
        test: /.tsx?$/,
        loader: 'ts-loader', // or 'awesome-typescript-loader'
        options: {
          getCustomTransformers: () => ({
            before: [tsClosureTransform.beforeTransform()],
            after: [tsClosureTransform.afterTransform()]
          })
        }
      }
    ]
  },
  resolve: {
    extensions: [ '.tsx', '.ts', '.js' ]
  },
  output: {
    path: path.join(__dirname, 'dist')
  }
}
