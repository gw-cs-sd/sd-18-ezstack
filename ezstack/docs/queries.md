# EZQL
EZQL is EZstack query language. This is the primary way users can
interact with EZapp. Users can leverage EZQL to query the datastore.

The `/sor/1/_search` endpoint expects a query object to be passed to it.

#### Query Object Properties

| Property | Type | Required | Default Value | Description |
| --- | --- | --- | --- | --- |
| searchTypes | <List> SearchType | | search | Allows you to define the aggregations or type of search. More on this can be found under the SearchType Object. |
| table | <String> table name | Yes | | is the table name that you are attempting to query. |
| filter | <List> Filter | | | Allows you to filter the query. Similar to `where` clause in SQL. More on this can be found under the Filter Object. |
| join | <Query> Nested Queries | Yes* | | Enables you to nest queries. Similar to inner join in SQL. |
| joinAttributeName | <String> attribute name | | _joinAttribute | Enables you to specify the attribute name that the `join` query result will be under. |
| joinAttributes | <List> JoinAttribute | Yes* | | Specifies the attributes between the top query and the inner query should be joined on. More on this can be found under JoinAttribute. |
| excludeAttributes | <List> exclude attributes | | | Set of string attributes that should be removed from document. |
| includeAttributes | <List> include attributes | | | Set of string attributes that should be included from the document.  Similar to SQL SELECT specific column names. |

\* Indicates that parameter is optional, however if `join` is supplied than `joinAttributes` must also be supplied.

#### SearchType Object Properties

| Property | Type | Required | Description |
| --- | --- | --- | --- |
| type | <Enum> search type | Yes | the type of search operation. The following operations are supported: {search, count, max, min, avg, sum} |
| attributeOn | <String> attribute | Yes* | attribute to apply the search type on. The only one that doesn't need an attribute is **search** because it simply states that you want to retrieve the documents rather than do aggregations on them. |


#### Filter Object Properties

| Property | Type | Required | Description |
| --- | --- | --- | --- |
| attribute | <String> attribute | Yes | Attribute to filter on. |
| op | <Enum> string operation | Yes | Operation to execute on filter. Supported operations both in long notation and mathematical form: {eq (==), not_eq (!=), gt (>), gte (>=), lt (<), lte (<=)} |
| value | <Object> value | Yes | static value the filter is comparing the document attribute against. |


#### JoinAttribute Object Properties

| Property | Type | Required | Description |
| --- | --- | --- | --- |
| outerAttribute | <String> attribute | Yes | The outer query table attribute that is being joined on. Such as primary key. |
| innerAttribute | <String> attribute | Yes | The inner query table attribute that is being joined on. Such as foreign key. |

#### EZQL Example Queries

**Simples EZQL query**
```json
{
    "table" : "tableName"
}
```

**SQL equivalent**
```sql
SELECT * FROM tableName;
```

<br><br>
**More advanced EZQL query**
```json
{
    "table" : "teacher",
    "filter" : [
        {
            "attribute" : "firstName",
            "op" : "eq",
            "value" : "gabe"
        },
        {
            "attribute" : "lastName",
            "op" : "eq",
            "value" "parmer"
        }
    ],
    "join" : {
        "searchTypes" : [
                {
                    "type" : "search"
                },
                {
                    "type" : "min",
                    "attributeOn" : "age"
                }
            ],
        "table" : "student"
    },
    "joinAttributes" : [
        {
            "outerAttribute" : "id",
            "innerAttribute" : "teacherId"
        }
    ]
}
```

**SQL equivalent**
```sql
SELECT *, MIN(Student.age)
FROM Teacher
INNER JOIN Teacher.id == Student.teacherId
WHERE Teacher.firstName == "gabe" AND Teacher.lastName == "parmer";
```
