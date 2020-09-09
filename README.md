# Squirrelistic Google Books Reducer #

## TLDR ##

Reduces the size of Google Books statistics by aggregating the years data and filtering the garbage.
Saves data to SQL Server database directly from compressed gzip files.

## Overview ##

Google scanned a lot of books and shared the resulting statistics files.
They are not evil after all.
The latest dataset from year 2020 is [available here](http://storage.googleapis.com/books/ngrams/books/datasetsv3.html])

The problem is that the dataset is humongous, especially for English language.
The table below describes humongousness of the data (version 20200217).

|Language   |File Count 1|Length 1 GB|File Count 2|Length 2 GB|File Count 3|Length 3 GB|File Count 4|Length 4 GB|File Count 5|Length 5 GB|File Count Sum|Length Sum GB|
|-----------|------------|-----------|------------|-----------|------------|-----------|------------|-----------|------------|-----------|--------------|-------------|
|eng        |24          |12.5       |589         |301.59     |6881        |3124.06    |6668        |2815.39    |19423       |7617.6     |33585         |13871.13     |
|eng-us     |14          |7.66       |365         |193.5      |4178        |1965.03    |3936        |1725.86    |11145       |4542.59    |19638         |8434.63      |
|eng-gb     |4           |3.69       |118         |88.96      |1283        |824.04     |1154        |685.01     |3098        |1688.82    |5657          |3290.52      |
|eng-fiction|1           |0.88       |47          |29.2       |549         |281.86     |515         |240.91     |1449        |613.76     |2561          |1166.6       |
|chi_sim    |1           |0.06       |6           |2.53       |59          |22.7       |46          |16.81      |105         |35.87      |217           |77.97        |
|fre        |6           |4.9        |115         |91.99      |1197        |806.99     |1104        |673.92     |3071        |1696.38    |5493          |3274.19      |
|ger        |8           |7.28       |181         |135.35     |1369        |902.17     |1003        |628.03     |2262        |1325.2     |4823          |2998.02      |
|heb        |1           |0.63       |10          |8.5        |45          |34.25      |25          |19.17      |42          |29.85      |123           |92.4         |
|ita        |2           |2.79       |60          |58.54      |553         |436.84     |427         |310.59     |984         |651.81     |2026          |1460.57      |
|rus        |2           |2.13       |69          |46.51      |471         |275.48     |313         |175.14     |633         |331.66     |1488          |830.92       |
|spa        |3           |3          |73          |57.8       |688         |451.35     |571         |345.4      |1415        |786.8      |2750          |1644.35      |
|Total      |66          |45.51      |            |1014.45    |17273       |9124.77    |15762       |7636.22    |43627       |19320.34   |78361         |37141.29     |

Yes, there are a lot of numbers, which may be confusing at first glance but there are just two things for each dataset.
- How many files do I have to download?
- How big are the files in total?

For example:
- if you were to obtain 1-gram (single words) data statistics for English (eng row), you would have to download 24 files (File Count 1) and it would consume 12.5 GB of disk space (Length 1 GB)
- if you were to obtain 5-gram (5 words combination) data statistics for Spanish (spa row), you would have to download 1415 files (File Count 5) and it would consume 786.8 GB of disk space (Length 5 GB)
- if you were to feel sudden rush to hoard all the 2020 data for all the languages, you would have to download 78361 files (File Count Sum) and it would consume 37 TB of disk space (Length Sum GB). Good luck with that.

And this is just the space for compressed gzip files, which are useless unless you uncompress them and put them into database.
The unpacked files would consume ~4 times more space, database might shrink down the numerical data, so let's say your full English dataset database would take 20-25 TB of data.

A lot of space and CPU is needed. And not everyone has a 40TB Hadoop cluster under his/her desk.
That is where Squirrelistic Google Books Reducer (SGBR) comes to alleviate the pain.

## How does the SGBR reduction work? ##

The SGBR works with Google books files version 20200217 only.
Typical source data row looks like this (but longer with more years included):

**Happy_ADJ Squirrel_NOUN  1816,1,1  1970,3,2  1971,11,8  1972,1,1  1973,2,1  1998,6,2**

Meaning 'Happy Squirrel' phrase where word Happy was used as an adjective and word Squirrel as a noun (can be a verb) was found in:
- 1 time in 1 book from year 1816
- 3 times in 2 books from year 1970
and so on.

The reduced row looks like this:

**Happy Squirrel	JN	24	15	1816	1998	6	1971	11**

Ngram: Happy Squirrel
Tags: JN (Adjective Noun) - see *[sgbr\Model\WordTag.cs](sgbr\Model\WordTag.cs)* for a complete list of tag letters.<br/>
Match Count: 24 - number of times phrase was found in books (all years).
Volume Count: 15 - number of books phrase was found in.
First Year: 1816 - first year the phrase was found in book.
Last Year: 1998 - last year the phrase was found in book.
Year Count: 6 - number of years.
Top Year: 1971 - first year where the phrase had the biggest number of matches.
Top Year Match Count: 6 number of times phrase was found in books in Top Year (1971 in this example).

The reducer takes *.gz file or URL as an input and save reduced *.gz file.

## How smaller are the files after reduction? ##

7-10 times smaller for English language (1,2,3-ngrams):

|Language   |Original 1|Reduced 2|Original 2|Reduced 2|Original 3|Reduced 3|
|-----------|----------|---------|----------|---------|----------|---------|
|eng        |12.5 GB   |1.28 GB  |301.59 GB |34.4 GB  |3124 GB   |417 GB   |

You can specify filters to reduce data further.

## Usage ##

### Reduce ###

Reduce the previously downloaded input file:

```Shell
sgbr reduce -i "C:\Stats\1-00005-of-00024.gz" -o "C:\Stats\1-00005-of-00024.reduced.gz"
```

Note that the reduced files have a header in the first line, whereas the original Google books files do not.

Reduce data from an URL (downloads and reduces on-the-fly):

```Shell
sgbr reduce -u "http://storage.googleapis.com/books/ngrams/books/20200217/eng-us/1-00005-of-00014.gz" -o "C:\Stats\1-00005-of-00014.reduced.gz"
```

Reduce further with filters (input file can be in original Google format or already reduced):

```Shell
sgbr reduce -i "C:\Stats\1-00005-of-00024.reduced.gz" -o "C:\Stats\1-00005-of-00024.reduced-more.gz" --length-filter 10 --tag-filter AN --no-letters-filter
```

--length-filter 10 ==> remove lines where any word in ngram has length longer than 10 characters
--tag-filter AN ==> remove lines with specific tags from the output (AN = Adverb Noun)
--no-letters-filter ==> remove lines where any word has no letters (e.g. all numbers)

Note:
- you can reduce with data from an URL, but it is better to keep unfiltered files and filter in next stage (in case some filtered data are needed after all).
- filters can also be specified when importing data to MSSQL database.

Reduce all files in specific directory into a single output file:

```Shell
sgbr reduce -d "C:\Stats" -o "C:\Output\combined-files.gz"
```

### Save data to SQL Server database ###

Currently on MS SQL Server is supported.

```Shell
sgbr save-to-sql -i "C:\Stats\1-00005-of-00024.reduced.gz" -c "Server=.;Database=GoogleNgrams;Integrated Security=true;"
```

This will create the following SQL objects if they do not exist:
- Words table ==> All words dictionary
- NgramStats1 table ==> Statistics data
- NgramWordsStats1 view ==> Joins Words and NgramStats1 tables for easier query

You can customise names of the table with -w (--words-table-name) and -s (--stats-prefix) parameters:

```Shell
sgbr save-to-sql -i "C:\Stats\1-00005-of-00024.reduced.gz" -c "Server=.;Database=GoogleNgrams;Integrated Security=true;" -w "EngWords" -s "Eng"
```

The table with words will be named EngWords, stats table EngStats1 and the view EngWordsStats1.

```Shell
sgbr save-to-sql -i "C:\Stats\1-00005-of-00024.reduced.gz" -c "Server=.;Database=GoogleNgrams;Integrated Security=true;" -w "EngWords" -s "Eng"
```

The preferred method for database save is to import files from a single directory in one go.

```Shell
sgbr save-to-sql -d "C:\Stats1files" -c "Server=.;Database=GoogleNgrams;Integrated Security=true;"
```

The is faster than importing each file separately because with each run the SGBR need to load Words table into memory.

### Speeding it up ###

SGBR uses main thread for data reading/download, separate thread for processing, and separate thread for saving data to output or database.
You can customise number of processing threads using -p flag e.g. 

```Shell
sgbr reduce -i "C:\Stats\1-00005-of-00024.gz" -o "C:\Stats\1-00005-of-00024.reduced.gz" -p 5
```

On 8-core CPU this reduced the processing time from 1 minute to around 25 seconds.

You can use the same flag for SQL import:

```Shell
sgbr save-to-sql -d "C:\Stats1files" -c "Server=.;Database=GoogleNgrams;Integrated Security=true;" -p 5
```

### Troubleshooting ###

Use "--log-level Debug" flag.

```Shell
sgbr reduce -i "C:\Stats\1-00005-of-00024.gz" -o "C:\Stats\1-00005-of-00024.reduced.gz" --log-level Debug
```
