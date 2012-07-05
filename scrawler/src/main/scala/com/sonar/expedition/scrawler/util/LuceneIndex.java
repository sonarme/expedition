package com.sonar.expedition.scrawler.util;


    import java.io.IOException;
    import java.util.ArrayList;

    import org.apache.lucene.analysis.Analyzer;
    import org.apache.lucene.analysis.standard.StandardAnalyzer;
    import org.apache.lucene.document.Document;
    import org.apache.lucene.document.Field;
    import org.apache.lucene.index.IndexReader;
    import org.apache.lucene.index.IndexWriter;
    import org.apache.lucene.index.IndexWriterConfig;
    import org.apache.lucene.queryParser.ParseException;
    import org.apache.lucene.queryParser.QueryParser;
    import org.apache.lucene.search.IndexSearcher;
    import org.apache.lucene.search.Query;
    import org.apache.lucene.search.ScoreDoc;
    import org.apache.lucene.store.Directory;
    import org.apache.lucene.store.RAMDirectory;
    import org.apache.lucene.util.Version;

    public class LuceneIndex {

        static Directory directory ;
        // To store the Lucene index in your harddisk, you can use:
        //Directory directory = FSDirectory.open("/foo/bar/testindex");
        // Set the analyzer that you want to use for the task.
        static Analyzer analyzer ;
        // Creating Lucene Index; note, the new version demands configurations.
        static IndexWriterConfig config ;
        static IndexWriter writer;
        static private ArrayList<Document> docs;

        public void initialise() throws IOException, ParseException {
            // To store the Lucene index in RAM
            // Note: There are other ways of initializing the IndexWriter.
            // (see http://lucene.apache.org/java/3_5_0/api/all/org/apache/lucene/index/IndexWriter.html)

            // The new version of Documents.add in Lucene requires a Field argument,
            //  and there are a few ways of calling the Field constructor.
            //  (see http://lucene.apache.org/java/3_5_0/api/core/org/apache/lucene/document/Field.html)
            // Here I just use one of the Field constructor that takes a String parameter.


            directory = new RAMDirectory();
            docs = new ArrayList<Document>();


           /* Document doc1 = new Document();
            doc1.add(new Field("id", "Title Lucene in Action",Field.Store.YES, Field.Index.ANALYZED));
            doc1.add(new Field("content", "Lucene in Action",Field.Store.YES, Field.Index.ANALYZED));
            docs.add(doc1);


             Document doc2 = new Document();
             doc2.add(new Field("id", "Title Lucene for Dummies",Field.Store.YES, Field.Index.ANALYZED));
             doc2.add(new Field("content", "Lucene for Dummies",Field.Store.YES, Field.Index.ANALYZED));

             Document doc3 = new Document();
             doc3.add(new Field("id", "Title Managing Gigabytes",Field.Store.YES, Field.Index.ANALYZED));
             doc3.add(new Field("content", "Managing Gigabytes",Field.Store.YES, Field.Index.ANALYZED));

             Document doc4 = new Document();
             doc4.add(new Field("id", "Title The Art of Lucene", Field.Store.YES, Field.Index.ANALYZED));
             doc4.add(new Field("content", "The Art of Lucene Gigabytes", Field.Store.YES, Field.Index.ANALYZED));


             docs.add(doc1);
             docs.add(doc2);
             docs.add(doc3);
             docs.add(doc4);
            */

        }


        public void addItems(String key , String value) throws IOException {
            Document doc = new Document();
            doc.add(new Field("id", key, Field.Store.YES, Field.Index.ANALYZED));
            doc.add(new Field("content", value, Field.Store.YES, Field.Index.ANALYZED));
            //System.out.println(key +"----" + value);
            docs.add(doc);

        }

        public String search(String srchkey) throws IOException, ParseException {
            // Parse a simple query that searches for the word "lucene".
            // Note: you need to specify the fieldname for the query
            // (in our case it is "content").
            IndexReader reader = IndexReader.open(directory);
            IndexSearcher searcher = new IndexSearcher(reader);

           // 4. display results

            QueryParser parser = new QueryParser(Version.LUCENE_35, "content", analyzer);
            Query query = parser.parse("Lucene");

            // Search the Index with the Query, with max 1000 results
            ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;

            System.out.println(srchkey + hits.length);

            // Iterate through the search results
            for (int i=0; i<hits.length;i++) {
                // From the indexSearch, we retrieve the search result individually
                Document hitDoc = searcher.doc(hits[i].doc);
                // Specify the Field type of the retrieved document that you want to print.
                // In our case we only have 1 Field i.e. "content".
                System.out.println(hitDoc.get("content") + "--- "+ hitDoc.get("id"));
                return hitDoc.get("id");
            }
            return null;

        }

        public void closeWriter() throws IOException {
            analyzer = new StandardAnalyzer(Version.LUCENE_35);
            config = new IndexWriterConfig(
                    Version.LUCENE_35, analyzer);

            writer = new IndexWriter(directory, config);


            writer.addDocuments(docs);
            writer.close();

        }

        public void closeObjects() throws IOException {
           //directory.close();
        }
    }
