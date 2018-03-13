#include <stdio.h>
#include <stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"

typedef struct details{

	int fixcount;
	int pagenumber;
	char *filename;
	int dirtybit;
	char *bufferdata;
        int temp_counter;
	int lru;
	int used_lru;
	int hitcount;
		

}details;

typedef struct datavalue{

	char *dataval;
}datavalue;

SM_FileHandle fh;
int fifo_counter=0;

details det[10000];

datavalue dat[25];
int temp1 = 0; // used for dataval



/******************************************************************************************************
 *  Function Name: initBufferPool
 *
 *  Description:
 *      Initializing BufferPool
 *
 *  Parameter:
 *      BM_BufferPool *const bm, const char *const pageFileName, 
		  const int numPages, ReplacementStrategy strategy, 
		  void *stratData
 *
 *  Return:
 *      RC_OK
 *
 *  Variables:
 *      temp,initial
 *  Authors:
 *      Prasanna Shanmuganathan, Adithya Ramachandran and Vishal Bimal
 *
 *  History:
 *      Date            Search String                           Content
 *      --------------- --------------------------------------- ----------------------
 *      10-20-2016      None                                    Initialization
 *
 *
 ******************************************************************************************************/


RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
		  const int numPages, ReplacementStrategy strategy, 
		  void *stratData)
{

	
	int temp=0;
	int initial= -1;	
	
	bm->pageFile= (char*)pageFileName;
	bm->numPages= numPages; 
	
	bm->strategy = strategy;
	
	bm->NumWriteIO=0;
	bm->NumReadIO=0;
	int value=0;
	det->temp_counter=0;
        det->hitcount=0;
	
	while(temp<bm->numPages)
	{
				
		det[temp].fixcount=0;
		det[temp].pagenumber=initial;
		det[temp].dirtybit=0;
		
		det[temp].bufferdata= NULL;
		
		det[temp].lru= initial;
		
		temp++;			


	}

	//for(int i=0;i<25;i++)
	//{
	//	dat[i].dataval=NULL;
	//}
		
	return RC_OK;


}


/******************************************************************************************************
 *  Function Name: shutdownBufferPool
 *
 *  Description:
 *      Shutting the  BufferPool on request.
 *
 *  Parameter:
 *      BM_BufferPool *const bm
 *
 *  Return:
 *      RC_OK
 *
 *  Variables:
 *      temp,initial
 *  Authors:
 *      Prasanna Shanmuganathan, Adithya Ramachandran and Vishal Bimal
 *
 *  History:
 *      Date            Search String                           Content
 *      --------------- --------------------------------------- ----------------------
 *      10-20-2016      None                                    Initialization
 *
 *
 ******************************************************************************************************/

RC shutdownBufferPool(BM_BufferPool *const bm)
{
	
		
	int temp=0;	
		
	for(temp;temp<bm->numPages;temp++)
	{
	
		if(det[temp].fixcount ==0)
		{
			
			//printf("No pages\n");
			det[temp].bufferdata=NULL;
		} 

	}
	
	return RC_OK;

}


/******************************************************************************************************
 *  Function Name: forceFlushPool
 *
 *  Description:
 *      forceFlushPool causes all dirty pages  from the buffer pool to be written to disk.
 *
 *  Parameter:
 *      BM_BufferPool *const bm
 *
 *  Return:
 *      RC_OK
 *
 *  Variables:
 *      temp
 *  Authors:
 *      Prasanna Shanmuganathan, Adithya Ramachandran and Vishal Bimal
 *
 *  History:
 *      Date            Search String                           Content
 *      --------------- --------------------------------------- ----------------------
 *      10-21-2016      None                                    Initialization
 *
 *
 ******************************************************************************************************/


RC forceFlushPool(BM_BufferPool *const bm)
{
	int temp=0;
	for(temp;temp<bm->numPages;temp++)
	{
		if(det[temp].dirtybit==1)
		{
			
			
			
			writeBlock(det[temp].pagenumber, &fh, det[temp].bufferdata);
			det[temp].dirtybit=0;
			

		}

	}
	return RC_OK;

}


/******************************************************************************************************
 *  Function Name: forcePage
 *
 *  Description:
 *      forcePage should write the current content of the page back to the page file on disk
 *
 *  Parameter:
 *      BM_BufferPool *const bm, BM_PageHandle *const page
 *
 *  Return:
 *      RC_OK
 *
 *  Variables:
 *      temp,initial
 *  Authors:
 *      Prasanna Shanmuganathan, Adithya Ramachandran and Vishal Bimal
 *
 *  History:
 *      Date            Search String                           Content
 *      --------------- --------------------------------------- ----------------------
 *      10-21-2016      None                                    Initialization
 *
 *
 ******************************************************************************************************/


RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page){

	int temp=0;

	for(temp;temp<bm->numPages;temp++)
	{

		if(det[temp].pagenumber==page->pageNum)
		{
					
		
			
			writeBlock(det[temp].pagenumber, &fh, page->data);
			
			

		}
	}
	return RC_OK;
}


/******************************************************************************************************
 *  Function Name: markDirty
 *
 *  Description:
 *      Marks the page as dirty.
 *
 *  Parameter:
 *      BM_BufferPool *const bm, BM_PageHandle *const page
 *
 *  Return:
 *      RC_OK
 *
 *  Variables:
 *      temp
 *  Authors:
 *      Prasanna Shanmuganathan, Adithya Ramachandran and Vishal Bimal
 *
 *  History:
 *      Date            Search String                           Content
 *      --------------- --------------------------------------- ----------------------
 *      10-22-2016      None                                    Initialization
 *
 *
 ******************************************************************************************************/


RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page){
	
	
	int temp=0;
	for(temp;temp<bm->numPages;temp++)
	{
	
		if(det[temp].pagenumber==page->pageNum)
		{
			det[temp].dirtybit=1;
			
		}	
	}
	(bm->NumWriteIO) += 1;
	return RC_OK;
}


/******************************************************************************************************
 *  Function Name: getFrameContents
 *
 *  Description:
 *      Returns an array of page numbers that the page frames are holding.
 *
 *  Parameter:
 *      BM_BufferPool *const bm
 *
 *  Return:
 *      RC_OK
 *
 *  Variables:
 *      temp
 *  Authors:
 *      Prasanna Shanmuganathan, Adithya Ramachandran and Vishal Bimal
 *
 *  History:
 *      Date            Search String                           Content
 *      --------------- --------------------------------------- ----------------------
 *      10-22-2016      None                                    Initialization
 *
 *
 ******************************************************************************************************/


PageNumber *getFrameContents (BM_BufferPool *const bm){

	
	int temp=0;
	PageNumber *pn = (PageNumber *)malloc((bm->numPages)*sizeof(PageNumber));
	for(temp;temp<bm->numPages;temp++)
	{
		
		pn[temp]= det[temp].pagenumber;
	}
	return pn;


}


/******************************************************************************************************
 *  Function Name: getDirtyFlags
 *
 *  Description:
 *      Returns an array of bools of dirtybits the  frames hold.
 *
 *  Parameter:
 *      BM_BufferPool *const bm
 *
 *  Return:
 *      RC_OK
 *
 *  Variables:
 *      temp
 *  Authors:
 *      Prasanna Shanmuganathan, Adithya Ramachandran and Vishal Bimal
 *
 *  History:
 *      Date            Search String                           Content
 *      --------------- --------------------------------------- ----------------------
 *      10-23-2016      None                                    Initialization
 *
 *
 ******************************************************************************************************/


bool *getDirtyFlags (BM_BufferPool *const bm){

	int temp=0;
	bool *dirtyflags = (bool *)malloc((bm->numPages)*sizeof(bool));
	for(temp;temp<bm->numPages;temp++)
	{
		if(det[temp].dirtybit==1)
		{
			
			dirtyflags[temp]=1;
			
		}
		else
		{
			dirtyflags[temp]=0;
			
		}	
	}
	
	return dirtyflags;
}


/******************************************************************************************************
 *  Function Name: getFixCounts
 *
 *  Description:
 *      Returns an array of fixcount in each frame.
 *
 *  Parameter:
 *      BM_BufferPool *const bm
 *
 *  Return:
 *      RC_OK
 *
 *  Variables:
 *      temp
 *  Authors:
 *      Prasanna Shanmuganathan, Adithya Ramachandran and Vishal Bimal
 *
 *  History:
 *      Date            Search String                           Content
 *      --------------- --------------------------------------- ----------------------
 *      10-23-2016      None                                    Initialization
 *
 *
 ******************************************************************************************************/


int *getFixCounts (BM_BufferPool *const bm){

	int temp=0;
	int *fixcounts= (int *)malloc((bm->numPages)*sizeof(int));
	for(temp;temp<bm->numPages;temp++)
	{
		fixcounts[temp]=det[temp].fixcount;
	}

	return fixcounts;
	

}


/******************************************************************************************************
 *  Function Name: getNumReadIO
 *
 *  Description:
 *      Returns the number of pages that have been read from the disk.
 *
 *  Parameter:
 *      BM_BufferPool *const bm
 *
 *  Return:
 *      RC_OK
 *
 *  Variables:
 *      None
 *  Authors:
 *      Prasanna Shanmuganathan, Adithya Ramachandran and Vishal Bimal
 *
 *  History:
 *      Date            Search String                           Content
 *      --------------- --------------------------------------- ----------------------
 *      10-23-2016      None                                    Initialization
 *
 *
 ******************************************************************************************************/

int getNumReadIO (BM_BufferPool *const bm){

	return (bm->NumReadIO);
}


/******************************************************************************************************
 *  Function Name: getNumWriteIO
 *
 *  Description:
 *      Returns the number of pages that have been written.
 *
 *  Parameter:
 *      BM_BufferPool *const bm
 *
 *  Return:
 *      RC_OK
 *
 *  Variables:
 *      None
 *  Authors:
 *      Prasanna Shanmuganathan, Adithya Ramachandran and Vishal Bimal
 *
 *  History:
 *      Date            Search String                           Content
 *      --------------- --------------------------------------- ----------------------
 *      10-23-2016      None                                    Initialization
 *
 *
 ******************************************************************************************************/

int getNumWriteIO (BM_BufferPool *const bm){
	
	return (bm->NumWriteIO);
}


/******************************************************************************************************
 *  Function Name: unpinPage
 *
 *  Description:
 *      Unpinpage helps in unpinning a page that is requested.
 *
 *  Parameter:
 *      BM_BufferPool *const bm, BM_PageHandle *const page
 *
 *  Return:
 *      RC_OK
 *
 *  Variables:
 *      temp
 *  Authors:
 *      Prasanna Shanmuganathan, Adithya Ramachandran and Vishal Bimal
 *
 *  History:
 *      Date            Search String                           Content
 *      --------------- --------------------------------------- ----------------------
 *      10-23-2016      None                                    Initialization
 *
 *
 ******************************************************************************************************/

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page){
	
	int temp=0;
	
	
	for(temp;temp<bm->numPages;temp++)
	{
		
		if(det[temp].pagenumber==page->pageNum)
		{
		
			if(det[temp].dirtybit>0)
			{
				forcePage(bm,page);
				det[temp].fixcount -=1;
			}				
			else
			{
					
					det[temp].fixcount -=1;
			}			
		}

	}
	
	return RC_OK;
				
			
}

/******************************************************************************************************
 *  Function Name: pinPage
 *
 *  Description:
 *      Pinpage helps in pinning the page that is requested for.
 *
 *  Parameter:
 *      BM_BufferPool *const bm, BM_PageHandle *const page
 *
 *  Return:
 *      RC_OK
 *
 *  Variables:
 *      temp,flag,flag1,*s
 *  Authors:
 *      Prasanna Shanmuganathan, Adithya Ramachandran and Vishal Bimal
 *
 *  History:
 *      Date            Search String                           Content
 *      --------------- --------------------------------------- ----------------------
 *      10-21-2016      None                                    Initialization
 *	10-28-2016      None                                    Initialization
 *
 ******************************************************************************************************/



RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
	    const PageNumber pageNum)
{
	fh.fileName= bm->pageFile;
	
	int temp=0;
	int flag=0;
	int flag1=0;
	char *s;
	
	if(bm->strategy == RS_FIFO)
	{



		for(temp;temp<bm->numPages;temp++)
		{
		
			if(det[temp].pagenumber== pageNum)
			{
				flag1=1;
				
				det[temp].fixcount += 1;
				

				page->data = det[temp].bufferdata;
				page->pageNum = pageNum;
				break;
				
			}
		}

		
 		
	
		for(temp=0;temp<bm->numPages;temp++)
		{
			
			
			if(det[temp].pagenumber==(-1))
			{
				
			
				det[temp].bufferdata=(SM_PageHandle) malloc(PAGE_SIZE);
				
				readBlock(pageNum, &fh, det[temp].bufferdata);
				
	    			page->pageNum = pageNum;
            			
				
				
				
				page->data = det[temp].bufferdata;
			
			
            			det[temp].pagenumber = pageNum;
				
            			det[temp].dirtybit = 0;
            			det[temp].fixcount = 1;
            			bm->NumReadIO += 1;

				flag=1;
				
				break;				
							
		
			 }
		  }

		if(flag==0 && flag1!=1)
		{
			FIFO(bm,page,pageNum);
	

		}
		return RC_OK;
	}
	else
	{

		for(temp;temp<bm->numPages;temp++)
		{
				
			if(det[temp].pagenumber== pageNum)
			{
				flag1=1;
				
				det[temp].fixcount += 1;
				

				page->data = det[temp].bufferdata;
				page->pageNum = pageNum;
				det->hitcount += 1 ;
				det[temp].used_lru=det->hitcount;
								
				break;
				 
			}
		}

		
 		
	
		for(temp=0;temp<bm->numPages;temp++)
		{
			
			if(det[temp].pagenumber==(-1))
			{
				
				
				
				
				det[temp].bufferdata=(SM_PageHandle) malloc(PAGE_SIZE);
				
				readBlock(pageNum, &fh, det[temp].bufferdata);
				
	    			page->pageNum = pageNum;
            			
				
				page->data = det[temp].bufferdata;
				
			
            			det[temp].pagenumber = pageNum;
				
            			det[temp].dirtybit = 0;
            			det[temp].fixcount = 1;
            			bm->NumReadIO += 1;
				det->hitcount += 1;
				det[temp].used_lru= det->hitcount;
								

				flag=1;
				
				break;				
				
		
			 }
		  }

		if(flag==0 && flag1!=1)
		{
			det->hitcount+=1;			
			
			LRU(bm,page,pageNum);
	

		}
		return RC_OK;


	}		
	
	
}


/******************************************************************************************************
 *  Function Name: FIFO
 *
 *  Description:
 *      FIFO is a replacement strategy algorithm used to replace the existing pages in the bufferpool.
 *
 *  Parameter:
 *      BM_BufferPool *const bm, BM_PageHandle *const page
 *
 *  Return:
 *      none
 *
 *  Variables:
 *      none
 *  Authors:
 *      Prasanna Shanmuganathan, Adithya Ramachandran and Vishal Bimal
 *
 *  History:
 *      Date            Search String                           Content
 *      --------------- --------------------------------------- ----------------------
 *      10-21-2016      None                                    Initialization
	10-25-2016      None                                    Modified functionality code 
 *      10-28-2016      None                                    Modified functionality code
 *
 ******************************************************************************************************/


void FIFO(BM_BufferPool *const bm, BM_PageHandle *const page, 
	    const PageNumber pageNum)
{

	
	fifo_counter = (det->temp_counter) % bm->numPages;
				
				if(det[fifo_counter].fixcount ==0)
				{
					
						
					
					
					readBlock(pageNum, &fh, det[fifo_counter].bufferdata);
					
					page->data =  det[fifo_counter].bufferdata;
					page->pageNum= pageNum;
                		        det[fifo_counter].pagenumber= pageNum;
					det[fifo_counter].dirtybit=0;
       					det[fifo_counter].fixcount = 1;
          				bm->NumReadIO += 1;
			        	det->temp_counter+= 1;
				}
			
				else
				{
					fifo_counter+=1;
				        
					
					readBlock(pageNum, &fh, det[fifo_counter].bufferdata);
					
					page->data =  det[fifo_counter].bufferdata;
					page->pageNum= pageNum;
                		        det[fifo_counter].pagenumber= pageNum;
					det[fifo_counter].dirtybit=0;
       					det[fifo_counter].fixcount = 1;
          				bm->NumReadIO += 1;
					 det->temp_counter+= 1;
				}
			
			
			
	



}


/******************************************************************************************************
 *  Function Name: LRU
 *
 *  Description:
 *      LRU is a replacement strategy algorithm used to replace the existing pages in the bufferpool.
 *
 *  Parameter:
 *      BM_BufferPool *const bm, BM_PageHandle *const page
 *
 *  Return:
 *      none
 *
 *  Variables:
 *      flag1,flag2,i,j
 *  Authors:
 *      Prasanna Shanmuganathan, Adithya Ramachandran and Vishal Bimal
 *
 *  History:
 *      Date            Search String                           Content
 *      --------------- --------------------------------------- ----------------------
 *      10-21-2016      None                                    Initialization
 *      10-26-2016      None                                    Modified functionality code
 *	10-29-2016      None                                    Modified functionality code
 ******************************************************************************************************/


void LRU(BM_BufferPool *const bm, BM_PageHandle *const page, 
	    const PageNumber pageNum)
{

	
	int flag1=0;
	int flag2=0;
	int i,j;
        int holder;
        int least;
  for(i=0;i<bm->numPages;i++)
  {
				
    if(det[i].fixcount == 0)
    {
      holder= i;
      least = det[i].used_lru;
	
      break;
    }

  }

    for(i=holder + 1;i<bm -> numPages;i++)
    {
	
      if(det[i].used_lru < least)
      {
	
        holder = i;
	
	
	least = det[i].used_lru;
	
	
	
	
      }

    }

	readBlock(pageNum, &fh, det[holder].bufferdata);
			
			page->data =  det[holder].bufferdata;
			page->pageNum= pageNum;
                        det[holder].pagenumber= pageNum;
			det[holder].dirtybit=0;
       			det[holder].fixcount = 1;
			det[holder].used_lru=det->hitcount;
			bm->NumReadIO += 1;
  



}








