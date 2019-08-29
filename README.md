
# `Dataset`

* `DataFrame` is a collection of `DataSet[Row]`
* Datasets are a strictly Java Virtual Machine (JVM) language feature
    * Work only with Scala and Java
* Can use an internal model representation structure 
  * For Scala that would be a `case class`
  * For Java that would be a Java Bean
  * There is slower performance with `DataSet` than a `DataFrame` due to conversion to custom Java objects
  * When using `case class`es it trivial to reuse them for both distributed and local workloads


## DataSets are DataFrames

* When reading in the data, `DataFrames` are `Dataset[Row]`
* This is done as a type alias `type DataFrame = Dataset[Row]`
* Therefore we can perform some functional programming like:
  * `map`
  * `flatMap`
  * `filter`
  * `foreach`

## Reasons for a `Dataset`
* Operations require functional programming solutions
* Rigorous Type Safety

### Creating the Schema


```scala
import org.apache.spark.sql.types._
val bookSchema = new StructType(Array(
   new StructField("bookID", IntegerType, false),
   new StructField("title", StringType, false),
   new StructField("authors", StringType, false),
   new StructField("average_rating", FloatType, false),
   new StructField("isbn", StringType, false),
   new StructField("isbn13", StringType, false),
   new StructField("language_code", StringType, false),
   new StructField("# num_pages", IntegerType, false),
   new StructField("ratings_count", IntegerType, false),
   new StructField("text_reviews_count", IntegerType, false)))
```


    Intitializing Scala interpreter ...



    Spark Web UI available at http://80f603593935:4040
    SparkContext available as 'sc' (version = 2.4.3, master = local[*], app id = local-1564085782545)
    SparkSession available as 'spark'
    





    import org.apache.spark.sql.types._
    bookSchema: org.apache.spark.sql.types.StructType = StructType(StructField(bookID,IntegerType,false), StructField(title,StringType,false), StructField(authors,StringType,false), StructField(average_rating,FloatType,false), StructField(isbn,StringType,false), StructField(isbn13,StringType,false), StructField(language_code,StringType,false), StructField(# num_pages,IntegerType,false), StructField(ratings_count,IntegerType,false), StructField(text_reviews_count,IntegerType,false))
    



### Creating column names to match the `case class`


```scala
val columnNames = Seq("bookID", "title", "authors",
      "averageRating", "isbn",
      "isbn13", "languageCode", "numPages", "ratingsCount",
      "textReviewsCount")
```




    columnNames: Seq[String] = List(bookID, title, authors, averageRating, isbn, isbn13, languageCode, numPages, ratingsCount, textReviewsCount)
    



### Read the file


```scala
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset

val dataset = spark
      .read
      .option("header", "true")
      .option("encoding", "UTF-8")
      .schema(bookSchema)
      .csv("../data/books.csv")
      .toDF(columnNames:_*) //Rename Columns
      .na.drop()            //Drop NA Values
dataset.show(10)
```

    +------+--------------------+--------------------+-------------+----------+-------------+------------+--------+------------+----------------+
    |bookID|               title|             authors|averageRating|      isbn|       isbn13|languageCode|numPages|ratingsCount|textReviewsCount|
    +------+--------------------+--------------------+-------------+----------+-------------+------------+--------+------------+----------------+
    |     1|Harry Potter and ...|J.K. Rowling-Mary...|         4.56|0439785960|9780439785969|         eng|     652|     1944099|           26249|
    |     2|Harry Potter and ...|J.K. Rowling-Mary...|         4.49|0439358078|9780439358071|         eng|     870|     1996446|           27613|
    |     3|Harry Potter and ...|J.K. Rowling-Mary...|         4.47|0439554934|9780439554930|         eng|     320|     5629932|           70390|
    |     4|Harry Potter and ...|        J.K. Rowling|         4.41|0439554896|9780439554893|         eng|     352|        6267|             272|
    |     5|Harry Potter and ...|J.K. Rowling-Mary...|         4.55|043965548X|9780439655484|         eng|     435|     2149872|           33964|
    |     8|Harry Potter Boxe...|J.K. Rowling-Mary...|         4.78|0439682584|9780439682589|         eng|    2690|       38872|             154|
    |     9|Unauthorized Harr...|W. Frederick Zimm...|         3.69|0976540606|9780976540601|       en-US|     152|          18|               1|
    |    10|Harry Potter Coll...|        J.K. Rowling|         4.73|0439827604|9780439827607|         eng|    3342|       27410|             820|
    |    12|The Ultimate Hitc...|       Douglas Adams|         4.38|0517226952|9780517226957|         eng|     815|        3602|             258|
    |    13|The Ultimate Hitc...|       Douglas Adams|         4.38|0345453743|9780345453747|         eng|     815|      240189|            3954|
    +------+--------------------+--------------------+-------------+----------+-------------+------------+--------+------------+----------------+
    only showing top 10 rows
    
    




    import org.apache.spark.sql.Row
    import org.apache.spark.sql.Dataset
    dataset: org.apache.spark.sql.DataFrame = [bookID: int, title: string ... 8 more fields]
    




```scala
dataset.filter(_.getAs[String]("title")
                 .contains("Fahrenheit"))
                 .show(10)
```

    +------+--------------------+--------------------+-------------+----------+-------------+------------+--------+------------+----------------+
    |bookID|               title|             authors|averageRating|      isbn|       isbn13|languageCode|numPages|ratingsCount|textReviewsCount|
    +------+--------------------+--------------------+-------------+----------+-------------+------------+--------+------------+----------------+
    |  4381|      Fahrenheit 451|Ray Bradbury-Alfr...|         3.98|0307347974|9780307347978|         spa|     175|      690801|           14489|
    |  4382|      Fahrenheit 451|Ray Bradbury-Chri...|         3.98|078617627X|9780786176274|         eng|       5|         471|             142|
    |  7656|      Fahrenheit 451|        Ray Bradbury|         3.98|8445074873|9788445074879|         eng|     186|        5733|             613|
    | 32971|      Fahrenheit 451|        Ray Bradbury|         3.98|0965020592|9780965020596|         eng|     190|         185|              26|
    | 32972|      Fahrenheit 451|        Ray Bradbury|         3.98|0345023021|9780345023025|         eng|     147|         208|              30|
    | 32973|      Fahrenheit 451|Ray Bradbury-Fran...|         3.98|9505470010|9789505470013|         spa|     263|         173|              23|
    | 37683|      Fahrenheit 451|Ray Bradbury-Alfr...|         3.98|8497930053|9788497930055|         spa|     176|         574|              64|
    | 40582|Michael Moore's F...| Robert Brent Toplin|         3.38|0700614524|9780700614523|         eng|     161|           8|               1|
    | 40694|Ray Bradbury's Fa...|        Harold Bloom|         4.18|0791059294|9780791059296|         eng|     147|         938|              47|
    +------+--------------------+--------------------+-------------+----------+-------------+------------+--------+------------+----------------+
    
    

### Creating the `case class`

* While operating in rows is acceptable
* For a static typed experience you can map the `Row` to a `case class`
* The `case class` should match the first names, and may require renaming to do so


```scala
case class Book(bookID: Int,
                title: String,
                authors: String,
                averageRating: Float,
                isbn: String,
                isbn13: String,
                languageCode:String,
                numPages: Int,
                ratingsCount: Int,
                textReviewsCount: Int)
```




    defined class Book
    




```scala
/* The imports are required for use */
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset

val bookDataset: Dataset[Book] = spark
      .read
      .option("header", "true")
      .option("encoding", "UTF-8")
      .schema(bookSchema)
      .csv("../data/books.csv")
      .toDF(columnNames:_*) //Rename Columns
      .na.drop()            //Drop NA Values
      .as[Book]             //Conversion to Case Class
bookDataset.show(10)
```

    +------+--------------------+--------------------+-------------+----------+-------------+------------+--------+------------+----------------+
    |bookID|               title|             authors|averageRating|      isbn|       isbn13|languageCode|numPages|ratingsCount|textReviewsCount|
    +------+--------------------+--------------------+-------------+----------+-------------+------------+--------+------------+----------------+
    |     1|Harry Potter and ...|J.K. Rowling-Mary...|         4.56|0439785960|9780439785969|         eng|     652|     1944099|           26249|
    |     2|Harry Potter and ...|J.K. Rowling-Mary...|         4.49|0439358078|9780439358071|         eng|     870|     1996446|           27613|
    |     3|Harry Potter and ...|J.K. Rowling-Mary...|         4.47|0439554934|9780439554930|         eng|     320|     5629932|           70390|
    |     4|Harry Potter and ...|        J.K. Rowling|         4.41|0439554896|9780439554893|         eng|     352|        6267|             272|
    |     5|Harry Potter and ...|J.K. Rowling-Mary...|         4.55|043965548X|9780439655484|         eng|     435|     2149872|           33964|
    |     8|Harry Potter Boxe...|J.K. Rowling-Mary...|         4.78|0439682584|9780439682589|         eng|    2690|       38872|             154|
    |     9|Unauthorized Harr...|W. Frederick Zimm...|         3.69|0976540606|9780976540601|       en-US|     152|          18|               1|
    |    10|Harry Potter Coll...|        J.K. Rowling|         4.73|0439827604|9780439827607|         eng|    3342|       27410|             820|
    |    12|The Ultimate Hitc...|       Douglas Adams|         4.38|0517226952|9780517226957|         eng|     815|        3602|             258|
    |    13|The Ultimate Hitc...|       Douglas Adams|         4.38|0345453743|9780345453747|         eng|     815|      240189|            3954|
    +------+--------------------+--------------------+-------------+----------+-------------+------------+--------+------------+----------------+
    only showing top 10 rows
    
    




    import org.apache.spark.sql.Row
    import org.apache.spark.sql.Dataset
    bookDataset: org.apache.spark.sql.Dataset[Book] = [bookID: int, title: string ... 8 more fields]
    



## Running some rudimentary functional programming 

* While it is better to run with our own custom types, we can also perform functional programming using `Row`
* Unfortunately for this example we cannot run it, due to this issue
https://github.com/Valassis-Digital-Media/spylon-kernel/issues/40
* Outside of a notebook we can run all the functional methods found in Scala in order to manipulate the data
  * `filter`
  * `flatMap`
  * `map`
  * etc.
* For example: `bookDataset.map(_.authors).show(10)`

## `flatMap` is powerful

* Given the `book` that is a `DataSet[Row]` one of the great things that we can with flatMap is explode records
* Recall that `flatMap` takes a single item and creates multiple as a function
* In our example the field `authors` has the potential for having multiple authors.
* If we look at the signature for `flatMap` it has:
   * `flatMap(scala.Function1<T,scala.collection.TraversableOnce<U>> func, Encoder<U> evidence)`
   * `TraversableOnce` is a supertype for most collections: `Seq`, `List`, `Vector`
* Any data engineer worth their salt must know `flatMap`
* In the next example, under `authors` there can be multiple authors or contributors that are separated by a dash.
  * For example, for many Harry Potter books, the authors are listed as "J.K. Rowling-Mary GrandPré"
  * We can give each person their own row, if we care to _with `flatMap`

Here is just a plain `map` that just takes the first author, which is handy


```scala
dataset.map{row => 
    val authors = row.getAs[String]("authors").split("-")
    (row.getInt(0),    //bookID
     row.getString(1), //title
     authors(0),       //author
     row.getFloat(3),  //averageRating
     row.getString(4), //isbn
     row.getString(5), //isbn13
     row.getString(6), //languageCode
     row.getInt(7),    //numPages
     row.getInt(8),    //ratingsCount
     row.getInt(9))    //textReviewsCount
}.toDF("bookID", "title", "author", "averageRating", "isbn", 
       "isbn13", "languageCode", "numPages", "ratingsCount", "textReviewsCount")
.show(10)
```

    +------+--------------------+--------------------+-------------+----------+-------------+------------+--------+------------+----------------+
    |bookID|               title|              author|averageRating|      isbn|       isbn13|languageCode|numPages|ratingsCount|textReviewsCount|
    +------+--------------------+--------------------+-------------+----------+-------------+------------+--------+------------+----------------+
    |     1|Harry Potter and ...|        J.K. Rowling|         4.56|0439785960|9780439785969|         eng|     652|     1944099|           26249|
    |     2|Harry Potter and ...|        J.K. Rowling|         4.49|0439358078|9780439358071|         eng|     870|     1996446|           27613|
    |     3|Harry Potter and ...|        J.K. Rowling|         4.47|0439554934|9780439554930|         eng|     320|     5629932|           70390|
    |     4|Harry Potter and ...|        J.K. Rowling|         4.41|0439554896|9780439554893|         eng|     352|        6267|             272|
    |     5|Harry Potter and ...|        J.K. Rowling|         4.55|043965548X|9780439655484|         eng|     435|     2149872|           33964|
    |     8|Harry Potter Boxe...|        J.K. Rowling|         4.78|0439682584|9780439682589|         eng|    2690|       38872|             154|
    |     9|Unauthorized Harr...|W. Frederick Zimm...|         3.69|0976540606|9780976540601|       en-US|     152|          18|               1|
    |    10|Harry Potter Coll...|        J.K. Rowling|         4.73|0439827604|9780439827607|         eng|    3342|       27410|             820|
    |    12|The Ultimate Hitc...|       Douglas Adams|         4.38|0517226952|9780517226957|         eng|     815|        3602|             258|
    |    13|The Ultimate Hitc...|       Douglas Adams|         4.38|0345453743|9780345453747|         eng|     815|      240189|            3954|
    +------+--------------------+--------------------+-------------+----------+-------------+------------+--------+------------+----------------+
    only showing top 10 rows
    
    

Here is `flatMap`, notice how in the result that _Mary GrandPré_ has her own row


```scala
dataset.flatMap{row => 
    val authors = row.getAs[String]("authors").split("-")
    authors.map(author => 
      (row.getInt(0),    //bookID
       row.getString(1), //title
       author,           //author
       row.getFloat(3),  //averageRating
       row.getString(4), //isbn
       row.getString(5), //isbn13
       row.getString(6), //languageCode
       row.getInt(7),    //numPages
       row.getInt(8),    //ratingsCount
       row.getInt(9)))    //textReviewsCount
}.toDF("bookID", "title", "author", "averageRating", "isbn", 
       "isbn13", "languageCode", "numPages", "ratingsCount", "textReviewsCount")
.show(10)
```

    +------+--------------------+-------------+-------------+----------+-------------+------------+--------+------------+----------------+
    |bookID|               title|       author|averageRating|      isbn|       isbn13|languageCode|numPages|ratingsCount|textReviewsCount|
    +------+--------------------+-------------+-------------+----------+-------------+------------+--------+------------+----------------+
    |     1|Harry Potter and ...| J.K. Rowling|         4.56|0439785960|9780439785969|         eng|     652|     1944099|           26249|
    |     1|Harry Potter and ...|Mary GrandPré|         4.56|0439785960|9780439785969|         eng|     652|     1944099|           26249|
    |     2|Harry Potter and ...| J.K. Rowling|         4.49|0439358078|9780439358071|         eng|     870|     1996446|           27613|
    |     2|Harry Potter and ...|Mary GrandPré|         4.49|0439358078|9780439358071|         eng|     870|     1996446|           27613|
    |     3|Harry Potter and ...| J.K. Rowling|         4.47|0439554934|9780439554930|         eng|     320|     5629932|           70390|
    |     3|Harry Potter and ...|Mary GrandPré|         4.47|0439554934|9780439554930|         eng|     320|     5629932|           70390|
    |     4|Harry Potter and ...| J.K. Rowling|         4.41|0439554896|9780439554893|         eng|     352|        6267|             272|
    |     5|Harry Potter and ...| J.K. Rowling|         4.55|043965548X|9780439655484|         eng|     435|     2149872|           33964|
    |     5|Harry Potter and ...|Mary GrandPré|         4.55|043965548X|9780439655484|         eng|     435|     2149872|           33964|
    |     8|Harry Potter Boxe...| J.K. Rowling|         4.78|0439682584|9780439682589|         eng|    2690|       38872|             154|
    +------+--------------------+-------------+-------------+----------+-------------+------------+--------+------------+----------------+
    only showing top 10 rows
    
    

## Lab: Functional Programming with DataSets
