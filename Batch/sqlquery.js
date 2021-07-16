const {
    Connection,
    Request,
    TYPES
} = require('tedious');
const fs = require('fs');
const moment = require('moment');
const { S3 } = require('aws-sdk');

let connection = null;

const getConnection = async () => {
    if (!connection) {
        const config = {
            server: 'database-1.c1yjaupfzfk9.us-east-1.rds.amazonaws.com',  //update me
            authentication: {
                type: 'default',
                options: {
                    userName: 'admin', //update me
                    password: 'test1234'  //update me
                }
            },
            options: {
                database: 'batchDb'
            }
        };
        connection = new Connection(config);
        connection.on('connect', async (err) => {
            console.log("Connected");

            // insert data 
            // let users = generateUserData(100, 2);
            // await loadBulkData(users);

            // get data
            await getPaginatedResponse();
        });
        connection.connect();
    }
}


async function inserData(rows) {
    const request = new Request(`INSERT guest.testDb (name, email, website, image) 
    VALUES 
    (@name, @email, @website, @image)`, function (err, rowCount, rows) {
        if (err) {
            console.log(err);
        }
    });
    request.on('row', function (columns) {
        columns.forEach(function (column) {
            if (column.value === null) {
                console.log('NULL');
            } else {
                console.log("Product id of inserted item is " + column.value);
            }
        });
    });

    // Close the connection after the final event emitted by the request, after the callback passes
    request.on("requestCompleted", function (rowCount, more) {
        connection.close();
    });
    connection.execSql(request);
}

async function loadBulkData(userData) {
    const table = '[guest].[testDb]';
    const option = { keepNulls: true }; // option to enable null values
    let bulkLoad = connection.newBulkLoad(table, option, (err, rowCont) => {
        if (err) {
            throw err;
        }
        console.log('rows inserted :', rowCont);
        console.log('DONE!');
        connection.close();
    });

    // setup columns
    bulkLoad.addColumn('name', TYPES.NVarChar, { nullable: true });
    bulkLoad.addColumn('email', TYPES.NVarChar, { nullable: true });
    bulkLoad.addColumn('website', TYPES.NVarChar, { nullable: true });
    bulkLoad.addColumn('image', TYPES.NVarChar, { nullable: true });
    // bulkLoad.addColumn('createdAt', TYPES.DateTime, inputData.createdAt);
    // add rows
    userData.forEach(user => {
        bulkLoad.addRow(user);
    })

    // perform bulk insert
    connection.execBulkLoad(bulkLoad);
}

async function getPaginatedResponse() {
    const fileName = './testing.txt';
    fs.writeFileSync(fileName, '');

    const request = new Request(`SELECT * FROM guest.testDb`, function (err) {
        if (err) {
            console.log(err);
        }
    });
    let result = "";
    request.on('row', function (columns) {
        const rows = columns.slice(0, 10);
        rows.forEach((column) => {
            if (column.metadata.colName === 'createdAt') {
                fs.appendFileSync(fileName, '-----------------\n');
            } else {
                console.log(`${column.metadata.colName}: ${column.value.toString()}`);
                fs.appendFileSync(fileName, `${column.metadata.colName}: ${column.value.toString()}\n`);
            }
        });
        console.log(result);
        result = "";
    });

    request.on('done', async function (rowCount, more) {
        console.log(rowCount + ' rows returned');
    });

    // Close the connection after the final event emitted by the request, after the callback passes
    request.on("requestCompleted", async function (rowCount, more) {
        // store file in s3
        await uploadFileInS3(fileName, `/batchFiles/${moment().format('YYYY-MM-DD')}.txt`)
        connection.close();
    });
    connection.execSql(request);
}

//---------------------------- generate data------------------------

const faker = require('faker');

const generateUserData = (numberOfRows, previousDays) => {
    const users = [];
    for (let i = 1; i <= numberOfRows; i++) {
        users.push({
            name: faker.name.findName(),
            email: faker.internet.email(),
            website: faker.internet.url(),
            image: faker.image.avatar(),
            // createdAt: moment().subtract(previousDays, 'day').format('YYYY-MM-DD HH:mm:ss')
        });
    }
    return users;
}


//---------------------------- upload file in s3 ------------------------
const s3 = new S3({
    accessKeyId: 'AKIA2CABUPZUINTM4U7M',
    secretAccessKey: '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',
    region: 'us-east-1'
});

const BucketName = 'moyeedbatches';
async function uploadFileInS3(filePath, s3Key) {
    const fileContent = fs.readFileSync(filePath);
    const params = {
        Bucket: BucketName,
        Key: s3Key,
        Body: fileContent
    }
    return new Promise((res, rej) => {
        s3.putObject(params, (err, data) => {
            if (err) {
                console.log("Erro while upload ", JSON.stringify(params), err);
                rej(err);
            } else {
                res(data);
            }
        })
    })
}




getConnection();