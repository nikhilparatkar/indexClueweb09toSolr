import java.util.List;
import java.lang.*;
import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;
public class MultiThreadedNewsGroupExecutor{
    String outputFilePath;
    String algorithm;
    String collectionPath;
    String solrCollectionName;

    public static void main(String[] args)
    {
        long startTime = System.currentTimeMillis();
        MultiThreadedNewsGroupExecutor obj = new MultiThreadedNewsGroupExecutor();
        /*Read input params here, pass them to prepareDocs */
        obj.readParameters();
        obj.initializeThreads();
        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        System.out.println("Time taken:"+elapsedTime);
    }

    public void initializeThreads(){
      File folder = new File(collectionPath);
      /*int numThreads = Runtime.getRuntime().availableProcessors();*/
      int numThreads = 1;
      DocumentIndexer[] threads = new DocumentIndexer[numThreads];
      File[] subFolders = folder.listFiles();

      int threadId = 0;
      /*Assign Folders to each thread
      * */
      for(int i = 0;i<subFolders.length;i++){
          if(threads[threadId]==null)
              threads[threadId] = new DocumentIndexer(outputFilePath,algorithm,subFolders[i].toString(),solrCollectionName);

          threadId += 1;
          threadId %= threads.length;
      }
      for(DocumentIndexer obj : threads)
        obj.start();
      try{
         for(DocumentIndexer obj : threads)
            obj.join();
      }catch(Exception ex){
          System.out.println("Exception caught: "+ex.toString());
      }
    }

    public void readParameters()
    {
        try{
            Properties prop = new Properties();
            InputStream inputStream = new FileInputStream(new File("config.properties"));
            if (inputStream != null) {
                prop.load(inputStream);
            } else {
                throw new FileNotFoundException("property file 'config.properties' not found in this path");
            }
            outputFilePath = prop.getProperty("outputFilePath");
            algorithm = prop.getProperty("algorithm");
            collectionPath  = prop.getProperty("collectionPath");
            solrCollectionName = prop.getProperty("solrCollectionName");
        }catch(Exception ex){
                System.out.println("Exception: " + ex);
        }
    }
}

class Document
{
    String docId;
    String docDirectory; /*this stores the folder name of the file. E.g.- "politics","entertainment",etc*/
    int clusterId;
    String trecId;
    String warcFileName;
    public Document(String docId,String docDirectory,int clusterId,String trecId,String warcFileName)
    {
        this.docId = docId;
        this.clusterId = clusterId;
        this.docDirectory = docDirectory;
        this.trecId = trecId;
        this.warcFileName = warcFileName;
    }
    @Override
    public String toString()
    {
        return String.format("|DocId:"+this.docId+" ClusterId:"+this.clusterId);
    }
}
class FileSearch
{
    String fileNameToSearch;
    String result;
    public void searchDirectory(File directory,String fileNameToSearch)
    {
        this.fileNameToSearch = fileNameToSearch;
        if(directory.isDirectory())
        {
            search(directory);
        }
        else
            System.out.println(directory.getAbsoluteFile());
    }
    public void search(File file)
    {
        if(file.canRead())
        {
            for(File tmp:file.listFiles())
            {
                if(tmp.isDirectory())
                    search(tmp);
                else if(fileNameToSearch.equals(tmp.getName().toLowerCase()))
                {
                    result = tmp.getAbsoluteFile().toString();
                }
            }
        }
        else
            System.out.println(file.getAbsoluteFile()+"  - Permission Denied!");
    }
}
class DocumentIndexer extends Thread{
    String outputFilePath;
    String algorithm;
    String collectionPath;
    String solrCollectionName;

    ArrayList<Document> documents;
    public DocumentIndexer(String outputFilePath,String algorithm,String collectionPath,String solrCollectionName){

        documents = new ArrayList<>();
        this.outputFilePath = outputFilePath;
        this.algorithm = algorithm;
        this.collectionPath = collectionPath;
        this.solrCollectionName = solrCollectionName;
        System.out.println("Created Document Indexer Thread with outputFilePath:"+outputFilePath+", collectionPath:"+collectionPath);
    }

    public void run(){
        System.out.println("\nRunning thread.");
        prepareDocs();
        /*simulateRun();*/
    }
    public void prepareDocs()
    {
        String line;
        int clusterId=999;
        /*ArrayList<Document> documents = new ArrayList<>();*/
        /*ArrayList<SolrInputDocument> solrInputDocs = new ArrayList<>();*/
        try
        {
          //Read each warc.gz file in the folder e.g. en0000/
          File folder = new File(collectionPath);
      		for (final File fileEntry : folder.listFiles())
      		{
      			//Call WarcReader method
            crawlWarc(fileEntry);
          }
        }catch(Exception ex)
        {
          System.out.println("!Exception ocured in prepareDocs():"+ex.toString());
        }
    }

    public void crawlWarc(File file){
    	try{
        	GZIPInputStream gzInputStream=new GZIPInputStream(new FileInputStream(file));
        	DataInputStream inStream=new DataInputStream(gzInputStream);
        	WarcRecord thisWarcRecord;
        	String	thisContentUtf8 = "";
          String warcTrecId = "";
          int clusterId;
          while ((thisWarcRecord=WarcRecord.readNextWarcRecord(inStream))!=null) {
        		if (thisWarcRecord.getHeaderRecordType().equals("response")) {
        			WarcHTMLResponseRecord htmlRecord=new WarcHTMLResponseRecord(thisWarcRecord);
        			warcTrecId = htmlRecord.getTargetTrecID().trim();

              clusterId = findClusterId(warcTrecId);
              if(clusterId!=9999){
        			   thisContentUtf8 = htmlRecord.getRawRecord().getContentUTF8();
                 //Add to document class
                 //call index to Solr?
                 String[] inputString ={"id",warcTrecId,"clusterId_i",Integer.toString(clusterId+1),"docContent_t",thisContentUtf8,"route","shard"+Integer.toString(clusterId+1)};
                 System.out.println("Doc:"+inputString[1]+" found in cluster:"+inputString[3]+"\n");
                 /*solrInputDocs.add(getSolrInputDoc(inputString));*/
              }
        		}
            thisContentUtf8 = "";
        	}
        	inStream.close();

    	}catch(Exception ex){
        System.out.println("!Exception ocured while reading document content:(crawlWarc) for file:"+file+"\n"+ex.toString());
      }
    }

    public int findClusterId(String warcTrecId){
  		int clusterId = 9999;
  		File folder = new File(outputFilePath);
  		for (final File fileEntry : folder.listFiles()){
        try{
          FileReader fileReader = new FileReader(fileEntry);
    			BufferedReader bufferedReader = new BufferedReader(fileReader);

    			String line = "";
          while((line = bufferedReader.readLine())!=null){
              if(!line.equals(" ")&&line.length()!=0){
    						if(line.trim().equals(warcTrecId)){
    							String[] tokens = fileEntry.getName().split("_");
    				      clusterId = Integer.parseInt(tokens[1].split("\\.")[0]);
    							System.out.println("Record Found in OutPutCluster:"+clusterId);
                  return clusterId;
    						}
          		}
    			}
    			bufferedReader.close();
        }catch(Exception e){
          System.out.println("Exception occured in findClusterId:"+warcTrecId+"\n"+e.toString());
        }
      }
      return clusterId;
    }


}
