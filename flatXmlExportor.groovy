import groovy.sql.Sql

/**
 * @author: linlei
 * @date: 16/9/1.
 */

@Grab('mysql:mysql-connector-java:5.1.24')
@GrabConfig(systemClassLoader = true)


/*********configuration begin*****************/
//db configuration
def db = [
        url     : 'jdbc:mysql://127.0.0.1:4001/my_database',
        user    : 'my_user_name',
        password: 'my_password',
        driver  : 'com.mysql.jdbc.Driver'
];
//the table to export data from
def table = 'table_name'
//the file to save the data(default use the table name)
def fileName = ''
/*********configuration end*****************/


def cli = new CliBuilder()
cli.with {
    h longOpt: 'help', 'Show usage information'
    t longOpt: 'table', args: 1, argName: 'table', required: false, 'the table to export data from'
    l longOpt: 'url', args: 1, argName: 'url', required: false, 'the url to the database'
    u longOpt: 'user', args: 1, argName: 'user', required: false, 'user name'
    p longOpt: 'password', args: 1, argName: 'password', required: false, 'password'
    d longOpt: 'driver', args: 1, argName: 'driverClass', required: false, 'the driver class used to access the database (default "com.mysql.jdbc.Driver")'
    f longOpt: 'file', args: 1, argName: 'file name', required: false, 'the file to save the data (default is table_name +".xml")'
}

def opt = cli.parse(args)
if (!opt) {
    return
}

if (opt.h) {
    cli.usage()
    return
}

if (opt.l) {
    db.url = opt.l
}

if (opt.u) {
    db.user = opt.u
}

if (opt.p) {
    db.password = opt.p
}

if (opt.d) {
    db.driver = opt.d
}

if (opt.t) {
    table = opt.t
}

if (opt.f) {
    fileName = opt.f
} else if (fileName.isEmpty()) {
    fileName = table
}

//create db connection
def sql = Sql.newInstance(db.url, db.user, db.password, db.driver);
def querySql = 'select * from ' + table


def builder = new groovy.xml.StreamingMarkupBuilder()
builder.encoding = "UTF-8"
builder.setUseDoubleQuotes(true)

//------------------------------------------------
def colNames = []

sql.eachRow("select COLUMN_NAME from information_schema.COLUMNS where table_name = $table " + "and table_schema = '" + db.url.substring(db.url.lastIndexOf('/') + 1) + "'") {
    r -> colNames << r['COLUMN_NAME']
}

println "All columns:"

colNames.eachWithIndex { elem, i ->
    println "$i : $elem"
}
println("input the column numbers (separated by comma) you don't want to export (press ENTER to skip):")
def str = System.in.newReader().readLine()
if (str.trim().length() > 0) {
    def num = str.split(',')
    def colTobeRemoved = []
    num.each { s -> colTobeRemoved << colNames[s.trim().toInteger()] }
    colNames.removeAll(colTobeRemoved)

    str = colNames.toString().replace('[', '').replace(']', '')
    querySql = 'select ' + str + ' from ' + table
}

println colNames
//------------------------------------------------
println()
println "Processing....Pls wait.........."
println()

def dataExport = {
    mkp.xmlDeclaration()
    dataset {
        sql.eachRow(querySql) {
            r ->
                map = [:]
                (0..<r.getMetaData().columnCount).collect {
                    col -> map[r.getMetaData().getColumnName(col + 1)] = r[col]
                }
                "$table"(map) 
        }
    }
}
def writer = new FileWriter("$fileName" + ".xml")
writer << builder.bind(dataExport)
println("Done!")
