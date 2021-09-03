const {LocalDate, nativeJs} = require("@js-joda/core");
const {generateSlug} = require("random-word-slugs");
const { Client } = require('pg')

// Database Connection info, Assumes columns, id(bigserial), values(jsonb)
const SCHEMA = 'test-big'
const TBL_NAME = 'entity_values'
const client = new Client({
  user: 'postgres',
  password: 'admin',
  host: 'localhost',
  port: 5432,
  database: 'postgres'
})

// -------------------------User Defined Constants-------------------------------------------
// Field info
const NUM_ROWS = 100000
const NUM_ROWS_PER_INSERT = 10000
const TYPE_POOL_SIZE = 20                      // Size of the type pool per assumption type.
const NUM_TYPE_POOL_ASSUMPTIONS = 2           // Number of assumptions that will pull from the type pool.

// Total number of assumptions for each type, including the pooled assumptions, Limit 899 per field type
const NUM_DATE_ASSUMPTIONS = 5                //2020-01-01 to 2099-12-31, id starts with 20
const NUM_TEXT_CODE_ASSUMPTIONS = 5           //5 to 10 chars, id starts with 30
const NUM_TEXT_SHORT_ASSUMPTIONS = 5          //10 to 40 chars, id starts with 31
const NUM_TEXT_LONG_ASSUMPTIONS = 5           //40 to 80 chars, id starts with 32
const NUM_NUMERIC_CURRENCY_ASSUMPTIONS = 20   //12 whole and 3 decimal, id starts with 40
const NUM_NUMERIC_FRACTION_ASSUMPTIONS = 10   //-1 to 1, 15 decimal places, id starts with 41
// ---------------------------------------------------------------------------------------------

function genRandNum(min = 1, max = 10000, precision = 0) {
  const seed = Math.random()*(max-min) + min
  const power = Math.pow(10, precision)
  return Math.floor(seed*power) / power
}

function genRandWholeNumber(min = 5, max = 10) {
  return Math.floor(Math.random() * (max-min + 1)) + min
}

function genRandDate(start = new Date('2020-01-01'), end = new Date('2099-12-31')) {
  return new Date(genRandNum(start.getTime(), end.getTime()))
}

function genRandString(min=5, max=10, whitelist='1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ') {
  let size = genRandWholeNumber()
  let result = ''
  for (let i = size; i > 0; i--) {
    result += whitelist[Math.floor(whitelist.length * Math.random())]
  }
  return result
}

function getId(prefix='00', assumptionIds=[]) {
  let id = prefix + genRandWholeNumber(100, 999)
  while(assumptionIds.includes(id)) {
    id = prefix + genRandWholeNumber(100, 999)
  }
  assumptionIds.push(id)
  return id
}

function createTypesPool() {
  const typesPool = {}
  for (const typesKey in types) {
    let floaties = []
    for (let i = 0; i < TYPE_POOL_SIZE; i++) {
      floaties.push(types[typesKey].gen())
    }
    typesPool[types[typesKey].type] = floaties
  }
  return typesPool
}

function createJsonb(schema, typesPool) {
  const jsonb = {}
  let idCounter = 0;
  for (const typesKey in types) {
    for (let i = 0; i < types[typesKey].num_assumptions; i++) {
      const id = schema[idCounter]
      idCounter++;
      if (i < NUM_TYPE_POOL_ASSUMPTIONS) {
        jsonb[id] = typesPool[types[typesKey].type][genRandWholeNumber(0,
            TYPE_POOL_SIZE - 1)]
      } else {
        jsonb[id] = types[typesKey].gen()
      }
    }
  }
  return jsonb;
}

function createSchema() {
  const schema = []
  for (const typesKey in types) {
    for (let i = 0; i < types[typesKey].num_assumptions; i++) {
      getId(types[typesKey].prefix, schema)
    }
  }
  return schema
}

const types = [{
  type: 'date',
  gen: () => LocalDate.from(nativeJs(genRandDate())).toString(),
  prefix: '20',
  num_assumptions: NUM_DATE_ASSUMPTIONS
}, {
  type: 'text_code',
  gen: () => genRandString(),
  prefix: '30',
  num_assumptions: NUM_TEXT_CODE_ASSUMPTIONS
}, {
  type: 'text_short',
  gen: () => generateSlug(genRandNum(1, 5), { format: 'title' }),
  prefix: '31',
  num_assumptions: NUM_TEXT_SHORT_ASSUMPTIONS
}, {
  type: 'text_long',
  gen: () => generateSlug(genRandNum(5, 15), { format: 'sentence' }),
  prefix: '32',
  num_assumptions: NUM_TEXT_LONG_ASSUMPTIONS
}, {
  type: 'numeric_currency',
  gen: () => genRandNum(1, 100000000000000, genRandNum(0, 3)),
  prefix: '40',
  num_assumptions: NUM_NUMERIC_CURRENCY_ASSUMPTIONS
}, {
  type: 'numeric_fraction',
  gen: () => genRandNum(-1, 1, genRandNum(1, 15)),
  prefix: '41',
  num_assumptions: NUM_NUMERIC_FRACTION_ASSUMPTIONS
}]

const main = async () => {
  try {
    await client.connect()

    await client.query(`DELETE FROM "${SCHEMA}"."${TBL_NAME}"`)

    const typesPool = createTypesPool();
    console.log('typesPool')
    console.log(typesPool)
    const schema = createSchema();
    console.log('Schema\n' + schema)

    let startTime = new Date().getTime();
    for (let insertNum = 0; insertNum < NUM_ROWS/NUM_ROWS_PER_INSERT; insertNum++) {
      let values = ''
      for (let row = 1; row <= NUM_ROWS_PER_INSERT; row++) {
        const rowJsonb = createJsonb(schema, typesPool)
        // const rowJsonb = {'foo': 'bar'}
        const rowId = row + NUM_ROWS_PER_INSERT * insertNum
        values += `(${rowId}, '${JSON.stringify(rowJsonb)}'),`
      }
      values = values.slice(0, -1) //remove last comma
      const insertSql = `INSERT INTO "${SCHEMA}"."${TBL_NAME}" (id, values)
                         VALUES ${values}`
      // console.log(insertSql)
      await client.query(insertSql)
    }

    let endTime = new Date().getTime()
    let dur = endTime - startTime
    console.log('Duration: ' + dur + ' milliseconds')
  } finally {
    client.end()
  }
}

void main()

// for (let i = 0; i < 10; i++) {
//   console.log(types[genRandNum(0, types.length-1)].gen())
// }
