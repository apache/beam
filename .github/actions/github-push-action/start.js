'use strict';
const spawn = require('child_process').spawn;
const path = require("path");
const https = require('https');

const get = (url, options = {}) => new Promise((resolve, reject) => https
    .get(url, options, (res) => {
        const chunks = [];
        res.on('data', (chunk) => chunks.push(chunk));
        res.on('end', () => {
            const body = Buffer.concat(chunks).toString('utf-8');
            if (res.statusCode < 200 || res.statusCode > 300) {
                return reject(Object.assign(
                    new Error(`Invalid status code '${res.statusCode}' for url '${url}'`),
                    { res, body }
                ));
            }
            return resolve(body)
        });
    })
    .on('error', reject)
)

const exec = (cmd, args = [], options = {}) => new Promise((resolve, reject) =>
    spawn(cmd, args, { stdio: 'inherit', ...options })
        .on('close', code => {
            if (code !== 0) {
                return reject(Object.assign(
                    new Error(`Invalid exit code: ${code}`),
                    { code }
                ));
            };
            return resolve(code);
        })
        .on('error', reject)
);

const trimLeft = (value, charlist = '/') => value.replace(new RegExp(`^[${charlist}]*`), '');
const trimRight = (value, charlist = '/') => value.replace(new RegExp(`[${charlist}]*$`), '');
const trim = (value, charlist) => trimLeft(trimRight(value, charlist));

const main = async () => {
    let branch = process.env.INPUT_BRANCH;
    const repository = trim(process.env.INPUT_REPOSITORY || process.env.GITHUB_REPOSITORY);
    if (!branch) {
        const headers = {
            'User-Agent': 'github.com/ad-m/github-push-action'
        };
        if (process.env.GITHUB_TOKEN) headers.Authorization = `token ${process.env.GITHUB_TOKEN}`;
        const body = JSON.parse(await get(`https://api.github.com/repos/${repository}`, { headers }))
        branch = body.default_branch;
    }
    await exec('bash', [path.join(__dirname, './start.sh')], {
        env: {
            ...process.env,
            INPUT_BRANCH: branch,
            INPUT_REPOSITORY: repository,
        }
    });
};

main().catch(err => {
    console.error(err);
    console.error(err.stack);
    process.exit(err.code || -1);
})
