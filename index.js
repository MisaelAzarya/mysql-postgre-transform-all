const mysql = require('mysql');
const pg = require('pg');
const Q = require("q");

const mysqlConnection = mysql.createConnection({
    host: "localhost",
    user: "root",
    password: "",
    database: "supersample"
})

//syntaxnya username:password@server:port/database_name
const pgConString = "postgres://postgres:1234@localhost:5432/supersample"

// function while menggunakan promise
function promiseWhile(condition, body) {
    var done = Q.defer();

    function loop() {
        if (!condition()) return done.resolve();
        Q.when(body(), loop, done.reject);
    }
    Q.nextTick(loop);

    // The promise
    return done.promise;
}

var clientpg = new pg.Client(pgConString);
clientpg.connect(function(err){
    if(err) throw err;
    console.log("connect");
    var pgTable = "CREATE TABLE IF NOT EXISTS orders (" +
                    "ID serial PRIMARY KEY," +
                    "City VARCHAR(255)," +
                    "This_Date VARCHAR(255)," +
                    "Sales NUMERIC(10,4) DEFAULT NULL," +
                    "That_Date VARCHAR(255)," +
                    "Prev_Sales NUMERIC(10,4) DEFAULT NULL," +
                    "Difference NUMERIC(10,4) DEFAULT NULL," +
                    "Percentage NUMERIC(10,4) DEFAULT NULL" +
                ");";
    
    clientpg.query(pgTable, function(err, results){
        if(err) throw err;
        else{
            console.log("Table Created");
            var flag = 1;
            var offset = 0;
            var count = 0;
            mysqlConnection.connect(function(err){
                if(err) throw err;
                console.log("Connected!");
                //selama datanya masih ada
                var bulanAwal = 4;
                var bulanBanding = bulanAwal-1;
                var tahunAwal = 2015;
                var tahunBanding = 2014;
                var place = 'city';
                promiseWhile(function () { return flag == 1; }, function () {
                    mysqlConnection.query(
                    'SELECT '+place+' "Kota", CONCAT(MONTH(order_date),"-",YEAR(order_date)) "This_Date", Sales "Sales_Bulan_Ini", CONCAT('+bulanBanding+',"-",'+tahunBanding+') "That_Date", '+
                    'CASE WHEN (SELECT Sales FROM orders WHERE '+place+' = o1.'+place+' AND MONTH(order_date) = '+bulanBanding+' AND YEAR(order_date) = '+tahunBanding+' GROUP BY '+place+') is NULL THEN 0 '+
                    'ELSE (SELECT Sales FROM orders WHERE '+place+' = o1.'+place+' AND MONTH(order_date) = '+bulanBanding+' AND YEAR(order_date) = '+tahunBanding+' GROUP BY '+place+') '+
                    'END AS "Sales_Bulan_Lalu", '+
                    'CASE WHEN (SELECT Sales FROM orders WHERE '+place+' = o1.'+place+' AND MONTH(order_date) = '+bulanBanding+' AND YEAR(order_date) = '+tahunBanding+' GROUP BY '+place+') is NULL THEN 0-Sales '+
                    'ELSE (SELECT Sales FROM orders WHERE '+place+' = o1.'+place+' AND MONTH(order_date) = '+bulanBanding+' AND YEAR(order_date) = '+tahunBanding+' GROUP BY '+place+')-Sales '+
                    'END AS "Selisih", '+
                    'ROUND(((SELECT Sales FROM orders WHERE '+place+' = o1.'+place+' AND MONTH(order_date) = '+bulanBanding+' AND YEAR(order_date) = '+tahunBanding+' GROUP BY '+place+') - Sales)/Sales * 100,2) "Persentase(%)" '+
                    'FROM orders o1 '+
                    'WHERE MONTH(order_date) = '+bulanAwal+' AND YEAR(order_date) = '+tahunAwal+' '+
                    'Group By '+place+' '+
                    'LIMIT '+offset+',1000',
                    function(err,rows,fields){
                        if(err) throw err;
                        else{
                            if(rows.length > 0){
                                const params = [];
                                rows.forEach(row => {
                                    const rowData = [];
                                    rowData.push('DEFAULT');
                                    fields.forEach(field => {
                                        if(row[field.name]==null){
                                            rowData.push('DEFAULT');
                                        }else{
                                            rowData.push("'" + row[field.name] + "'");
                                        }
                                    })
                                    params.push('(' + rowData.join(', ') + ')');
                                });

                                var pgInsert = "INSERT INTO orders VALUES "+params;
                                clientpg.query(pgInsert, function(err){
                                    if(err) throw err;
                                    count += rows.length;
                                    console.log("Running data "+count);
                                });
                            }
                            else{
                                // sudah tidak ada data
                                flag = 0;
                            }
                        }
                    });
                    offset += 1000;
                    return Q.delay(0); // arbitrary async
                }).then(function () {
                    console.log("Done");
                }).done();
            });
        }
    });
    
});
