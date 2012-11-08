package com.zvents.solr.components;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ScoreStatsPostFilterTest extends SolrTestCaseJ4
{
    @Before
    public void setup() throws Exception
    {
        super.setUp();
        initCore("solrconfig.xml", "schema.xml", "src/test/");
        //Let's add some documents
        for(int i=0; i < 100; i++)
        {
            double dFeature = i;
            assertU(adoc("id",i+"","name","Name of document","doc_feature",dFeature + ""));
        }
        assertU(commit());
    }
    
    @Test
    public void testSimple() throws Exception
    {
        assertQ(req("q","id:[* TO *]"), "/response/result/@numFound=100");
    }
    
    @Test
    public void testShouldContainScoreDistribution() throws Exception
    {
        //<lst name="scoreStats">
        SolrQueryRequest req = req("q","document","scoreDist","true","rows","100","fl","*,score");
        assertQ(req, "count(//lst[@name='scoreStats'])=1",
                "//lst[@name='scoreStats']/long[@name='numDocs']=100",
                "//lst[@name='scoreStats']/float[@name='avg']=19.001766",
                "//lst[@name='scoreStats']/float[@name='sumScores']=1900.1766",
                "//lst[@name='scoreStats']/float[@name='sumSquaredScores']=47799.43",
                "//lst[@name='scoreStats']/float[@name='stdDev']=10.813288");
    }

    @After
    public void tearDown() throws Exception
    {
        // TODO Auto-generated method stub
        super.tearDown();
        deleteCore();
    }
}
