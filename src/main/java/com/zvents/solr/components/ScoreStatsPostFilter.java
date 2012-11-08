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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorer;
import org.apache.solr.search.DelegatingCollector;
import org.apache.solr.search.ExtendedQueryBase;
import org.apache.solr.search.PostFilter;

/**
 * PostFilter that does the actual work of computing the basic stats of the documents 
 * that matched the query and were filtered out by the 
 * filter queries. This limits the work to only those documents that are actual matches.
 * 
 * @author Amit Nithianandan
 * ANithian-at-gmail.com
 *
 */
public class ScoreStatsPostFilter extends ExtendedQueryBase implements PostFilter
{
    private DistributionCalcCollector mCollector;
    
    private static class DistributionCalcCollector extends DelegatingCollector
    {
        private int mLastDocBase = -1;
        
        private class DistributionCalcScorer extends Scorer
        {
            private Scorer mDelegateScorer = null;
            //Stats to care about:
            //min, max, number of docs, sum, sum_squared
            
            private float m_fMinScore=Float.MAX_VALUE;
            private float m_fMaxScore = Float.MIN_VALUE;
            private float m_fSumScores;
            private float m_fSumSquaredScores;
            private int m_iNumDocs;
            
            private int mCurrentDoc = -1;
            private float mCurrentScore = 0f;
            
            private boolean m_bIsRolledOver = false;
            
            public DistributionCalcScorer(Scorer delegateScorer)
            {
                super(delegateScorer.getWeight());
                mDelegateScorer = delegateScorer;
                // TODO Auto-generated constructor stub
            }

            @Override
            public float score() throws IOException
            {
                // TODO Auto-generated method stub
                int iDoc = docID();
                //Prevent multiple instantiation of this from screwing up the metrics.
                if(iDoc != mCurrentDoc)
                {
                    float fScore= mDelegateScorer.score();
                    if(!m_bIsRolledOver)
                    {
                        m_iNumDocs++;
                        if(fScore < m_fMinScore)
                            m_fMinScore =fScore;
                        if(fScore > m_fMaxScore)
                            m_fMaxScore=fScore;
                        m_fSumScores+=fScore;
                        m_fSumSquaredScores+=(fScore*fScore);
                    }
                    mCurrentDoc = iDoc;
                    mCurrentScore = fScore;
                }
                return mCurrentScore;
            }

            @Override
            public float freq() throws IOException
            {
                // TODO Auto-generated method stub
                return mDelegateScorer.freq();
            }

            @Override
            public int docID()
            {
                // TODO Auto-generated method stub
                return mDelegateScorer.docID();
            }

            @Override
            public int nextDoc() throws IOException
            {
                // TODO Auto-generated method stub
                return mDelegateScorer.nextDoc();
            }

            @Override
            public int advance(int target) throws IOException
            {
                // TODO Auto-generated method stub
                return mDelegateScorer.advance(target);
            }
        }
        
        private DistributionCalcScorer mScorer;
        
        @Override
        public void setScorer(Scorer scorer) throws IOException
        {
            //The scorer is set for each segment it seems (leaves in reader context)? So let's make sure that we don't reset ourselves 
            //by creating a new scorer but instead associate the new scorer with the stats scorer.
            if(mScorer == null)
            {
                mScorer = new DistributionCalcScorer(scorer);
                super.setScorer(mScorer);
            }
            else
            {
                mScorer.mDelegateScorer=scorer;
                super.setScorer(mScorer);
            }
        }

        @Override
        public void setNextReader(AtomicReaderContext context)
                throws IOException
        {
            // Grouping f*s things up in that it won't visit all the groups.. only the number of those that were requested. Since this scorer is sensitive to multiple passes through the index, if we
            //see that we are visiting the documents *again*, let's tell the scorer to stop collecting stats. This is a problem with grouping b/c it visits documents again so we would be reporting stats on
            //documents not being returned to the client but oh well it's the best I can do.
            if(context.docBase <= mLastDocBase && mScorer != null)
            {
                mScorer.m_bIsRolledOver=true;
            }
            else
            {
                mLastDocBase = context.docBase;
            }
            super.setNextReader(context);
        }
    }
    
    @Override
    public boolean getCache()
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void setCache(boolean cache)
    {
        // TODO Auto-generated method stub

    }

    @Override
    public int getCost()
    {
        // TODO Auto-generated method stub
        return 101;
    }

    @Override
    public void setCost(int cost)
    {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean getCacheSep()
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void setCacheSep(boolean cacheSep)
    {
        // TODO Auto-generated method stub

    }

    public int getNumDocs()
    {
        if(mCollector == null || mCollector.mScorer == null)
            return -1;        
        return mCollector.mScorer.m_iNumDocs;
    }
    
    public float getMinScore()
    {
        if(mCollector == null || mCollector.mScorer == null)
            return Float.NaN;
        return mCollector.mScorer.m_fMinScore;
    }

    public float getMaxScore()
    {
        if(mCollector == null || mCollector.mScorer == null)
            return Float.NaN;
        return mCollector.mScorer.m_fMaxScore;
    }
    
    public float getSumScores()
    {
        if(mCollector == null || mCollector.mScorer == null)
            return Float.NaN;  
        return mCollector.mScorer.m_fSumScores;
    }

    public float getSumSquaredScores()
    {
        if(mCollector == null || mCollector.mScorer == null)
            return Float.NaN;          
        return mCollector.mScorer.m_fSumSquaredScores;
    }
    
    @Override
    public DelegatingCollector getFilterCollector(IndexSearcher searcher)
    {
        // TODO Auto-generated method stub
        mCollector= new DistributionCalcCollector();
        return mCollector;
    }
}
