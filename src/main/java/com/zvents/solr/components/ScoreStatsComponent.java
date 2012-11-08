package com.zvents.solr.components;

/*
 * Copyright (c) 2012 Zvents Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, 
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. 
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, 
DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, 
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER 
DEALINGS IN THE SOFTWARE.
 */


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.search.Query;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.search.QueryResultKey;
import org.apache.solr.search.SolrIndexSearcher.QueryCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Solr component that places some basic stats about the distribution of the scores as a named list. 
 * This gives some information about the documents and the tools necessary to compute some 
 * basic relative stats about how good a document is compared to its peers in this 
 * document set.
 * 
 * @author Amit Nithianandan
 * ANithian-at-gmail.com
 *
 */
public class ScoreStatsComponent extends SearchComponent
{
    //Cache to store the results b/c if this query hits the query results cache, then the collector won't get invoked so this
    //must be at least as big as the query result cache.
    private static final String CACHE_NAME="scoreStatsCache";
    public static final String SCORE_PARAM="scoreDist";
    private static Logger sm_log = LoggerFactory.getLogger(ScoreStatsComponent.class);
    
    @Override
    public String getDescription()
    {
        // TODO Auto-generated method stub
        return "Returns the statistics of the scores of the documents";
    }

    @Override
    public String getSource()
    {
        // TODO Auto-generated method stub
        return "";
    }

    @Override
    public void prepare(ResponseBuilder rb) throws IOException
    {
        boolean bComputeScores = rb.req.getParams().getBool(SCORE_PARAM, false);
        if(!bComputeScores)
            return;
        List<Query> lFilters = rb.getFilters();
        if(lFilters == null)
        {
            lFilters = new ArrayList<Query>();
            rb.setFilters(lFilters);
        }
        //Add the post filter.
        lFilters.add(new ScoreStatsPostFilter());
    }

    @Override
    public void process(ResponseBuilder rb) throws IOException
    {
        boolean bComputeScores = rb.req.getParams().getBool(SCORE_PARAM, false);
        if(!bComputeScores)
            return;
        
        //Find the postfilter and grab the stats
        List<Query> lFilters = rb.getFilters();
        if(lFilters != null)
        {
            for(Query q:lFilters)
            {
                if(q instanceof ScoreStatsPostFilter)
                {
                    ScoreStatsPostFilter pf = (ScoreStatsPostFilter)q;
                    QueryCommand cmd = rb.getQueryCommand();
                    QueryResultKey key = new QueryResultKey(cmd.getQuery(), cmd.getFilterList(), cmd.getSort(), cmd.getFlags());
                    
                    NamedList<Number> scoreStats = null;
                    long iNumDocs = pf.getNumDocs();
                    
                    if(iNumDocs < 0)
                    {
                        scoreStats = (NamedList<Number>)rb.req.getSearcher().cacheLookup(CACHE_NAME, key);
                    }
                    else
                    {
                        float fSumSquaredScores = pf.getSumSquaredScores();
                        float fAvg = pf.getSumScores()/iNumDocs;
                        float fMax = pf.getMaxScore();
                        float fVariance  = pf.getSumSquaredScores()/iNumDocs - fAvg*fAvg;
                        float fStdDev = 1.0f;
                        if (fVariance > 0)
                          fStdDev = (float)Math.sqrt(fVariance);
                        
                        //Basic stuff to calculate average, std-dev, min just there for information
                        scoreStats = new NamedList<Number>();
                        scoreStats.add("min", pf.getMinScore());
                        scoreStats.add("max", fMax);
                        scoreStats.add("avg", fAvg);
                        scoreStats.add("stdDev", fStdDev);
                        scoreStats.add("numDocs",iNumDocs);
                        scoreStats.add("sumSquaredScores", fSumSquaredScores);
                        scoreStats.add("sumScores", pf.getSumScores());
                        rb.req.getSearcher().cacheInsert(CACHE_NAME, key, scoreStats);
                    }
                    if(scoreStats != null)
                        rb.rsp.add("scoreStats", scoreStats);
                }
            }
        }        
    }

}
