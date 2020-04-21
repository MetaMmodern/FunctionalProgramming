const basicString1 = "SELECT Col1, Col2, Col3 FROM thisTable";
const basicString2 = "SELECT DISTINCT Col1, Col2, Col3 FROM thisTable";
const basicString3 = "SELECT Col1, Col2, Col3 FROM thisTable WHERE Col1 = 1";
const basicString4 =
  "SELECT DISTINCT Col1, Col2, Col3 FROM thisTable WHERE Col1 = 1";
const basicString5 = "SELECT AVG(Col1) FROM thisTable WHERE Col1 = 1";

function columnsGlue(cols) {}

function simpleColsParser(queryArray) {
  const columns = queryArray
    .slice(1, queryArray.indexOf("FROM"))
    .join("")
    .split(",");
  if (columns.join().includes("(") && columns.join().includes(")")) {
    //function for FUN();
  } else if (columns.length != 0) {
    //ordinary list of cols
    console.log("good");
    simpleSelect(columns);
  } else {
    throw new Error("some kind of error about columns");
  }
}

[[{ name: "Michael" }], [{ name: "Michael" }], [{ name: "Michael" }]];

const settings = ["*", "age", "favAnimal", "age "];
const table = [
  { name: "Michael", age: 25, favAnimal: "dog" },
  { name: "jhon", age: 20, favAnimal: "cat" },
  { name: "haha", age: 20, favAnimal: "wolf" },
  { name: "santa", age: 19, favAnimal: "fish" },
];

function simpleSelect(cols) {
  console.log(cols);
}

function distinctSelect(params) {}

function whereParser(params) {}

function whereFilter() {}

function getTableIndex(query) {
  const indexOfTable = query.indexOf("FROM") + 1;
  if (indexOfTable != -1) {
    return query[indexOfTable];
  } else {
    return "Error";
  }
}

function parseSQL(str) {
  const queryArray = str.split(" ");
  if (queryArray[0].toUpperCase() != "SELECT") {
    console.log("error: not sql");
    return;
  }
  const TableName = getTableIndex(queryArray);
  if (TableName === "Error") {
    throw new Error("TableName Error");
  }

  if (
    queryArray[1].toUpperCase() != "DISTINCT" ||
    queryArray[1].toUpperCase() != "CASE"
  ) {
    simpleColsParser(queryArray);
  } else {
    if (queryArray[1].toUpperCase() === "DISTINCT") {
      distinctSelect();
    }
    if (queryArray[1].toUpperCase() === "CASE") {
      //caseselect gonna be here later;
    }
  }
}

// parseSQL(basicString1);
myMapper();

function whereParser(whereIndex, table, query, sqlParams) {
  let allwheres = query.split(whereIndex, end);
  if (sqlParams.hasOrder) {
    allwheres = allwheres.withoutOrder;
  }
  if (sqlParams.hasGroup) {
    allwheres = allwheres.withoutGroup;
  }
}

function oneWhereDF(whereExpr, query, DF) {}

const wheres = "col1 > 3 and col2 = 1 or not col4 > 5";

function recursiveParewheres(wheres, clauseslist, andorlist, nowclause) {
  if (wheres.length == 0) {
    return [clauseslist.push(nowclause), andorlist];
  } else {
    switch (wheres.last) {
      case "and":
        recursiveParewheres(
          wheres.withoutlast,
          clauseslist.push(nowclause),
          andorlist.push("and"),
          []
        );
        break;
      case "or":
        recursiveParewheres(
          wheres.withoutlast,
          clauseslist.push(nowclause),
          andorlist.push("or"),
          []
        );
        break;
      default:
        recursiveParewheres(
          wheres.withoutlast,
          clauseslist,
          andorlist,
          wheres.last + nowclause
        );
        break;
    }
  }
}

const queryArray = str.toLowerCase().split(" ");

const sql = parseRequest("select * from foobar");

const tableDf = readTable(sql.selectTable);

const dfWithSelectedExpressions = applySelectExpressions(
  tableDf,
  sql.selectExpressions
);

const dfWithDistinct = sql.distinctSelect
  ? applyDistinct(dfWithSelectedExpressions)
  : dfWithSelectedExpressions;

const withOrdering = applyOrdering(dfWithDistinct, sql.ordering);

function getWhereDf(wheresArray, andorArray, table) {
  const firstDF = twoDFintoOne(
    oneExpressionParser(wheresArray[0], table),
    oneExpressionParser(wheresArray[1], table),
    andorArray[0]
  );
}
