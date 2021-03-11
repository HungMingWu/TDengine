#include "qResultbuf.h"
#include "stddef.h"
#include "tscompression.h"
#include "hash.h"
#include "qExtbuffer.h"
#include "queryLog.h"
#include "taoserror.h"

#define GET_DATA_PAYLOAD(_p) ((char *)(_p)->pData + POINTER_BYTES)
#define NO_IN_MEM_AVAILABLE_PAGES(_b) (listNEles((_b)->lruList) >= (_b)->inMemPages)

int32_t createDiskbasedResultBuffer(SDiskbasedResultBuf** pResultBuf, int32_t rowSize, int32_t pagesize,
                                    int32_t inMemBufSize, const void* handle) {
  *pResultBuf = (SDiskbasedResultBuf*)calloc(1, sizeof(SDiskbasedResultBuf));

  SDiskbasedResultBuf* pResBuf = *pResultBuf;
  if (pResBuf == NULL) {
    return TSDB_CODE_COM_OUT_OF_MEMORY;  
  }

  pResBuf->pageSize     = pagesize;
  pResBuf->numOfPages   = 0;                        // all pages are in buffer in the first place
  pResBuf->totalBufSize = 0;
  pResBuf->inMemPages   = inMemBufSize/pagesize;    // maximum allowed pages, it is a soft limit.
  pResBuf->allocateId   = -1;
  pResBuf->comp         = true;
  pResBuf->file         = NULL;
  pResBuf->handle       = handle;
  pResBuf->fileSize     = 0;

  // at least more than 2 pages must be in memory
  assert(inMemBufSize >= pagesize * 2);

  pResBuf->numOfRowsPerPage = (pagesize - sizeof(tFilePage)) / rowSize;
  pResBuf->lruList = tdListNew(POINTER_BYTES);

  // init id hash table
  pResBuf->assistBuf = malloc(pResBuf->pageSize + 2); // EXTRA BYTES
  pResBuf->all = taosHashInit(10, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);

  char path[PATH_MAX] = {0};
  taosGetTmpfilePath("qbuf", path);
  pResBuf->path = strdup(path);

  pResBuf->emptyDummyIdList = taosArrayInit(1, sizeof(int32_t));

  qDebug("QInfo:%p create resBuf for output, page size:%d, inmem buf pages:%d, file:%s", handle, pResBuf->pageSize,
         pResBuf->inMemPages, pResBuf->path);

  return TSDB_CODE_SUCCESS;
}

static int32_t createDiskFile(SDiskbasedResultBuf* pResultBuf) {
  pResultBuf->file = fopen(pResultBuf->path.c_str(), "wb+");
  if (pResultBuf->file == NULL) {
    qError("failed to create tmp file: %s on disk. %s", pResultBuf->path, strerror(errno));
    return TAOS_SYSTEM_ERROR(errno);
  }

  return TSDB_CODE_SUCCESS;
}

static char* doCompressData(void* data, int32_t srcSize, int32_t *dst, SDiskbasedResultBuf* pResultBuf) { // do nothing
  if (!pResultBuf->comp) {
    *dst = srcSize;
    return (char*)data;
  }

  *dst = tsCompressString((char*)data, srcSize, 1, (char*)pResultBuf->assistBuf, srcSize, ONE_STAGE_COMP, NULL, 0);

  memcpy(data, pResultBuf->assistBuf, *dst);
  return (char*)data;
}

static char* doDecompressData(void* data, int32_t srcSize, int32_t *dst, SDiskbasedResultBuf* pResultBuf) { // do nothing
  if (!pResultBuf->comp) {
    *dst = srcSize;
    return (char*)data;
  }

  *dst = tsDecompressString((char*)data, srcSize, 1, (char*)pResultBuf->assistBuf, pResultBuf->pageSize, ONE_STAGE_COMP, NULL, 0);

  memcpy(data, pResultBuf->assistBuf, *dst);
  return (char*)data;
}

static int32_t allocatePositionInFile(SDiskbasedResultBuf* pResultBuf, size_t size) {
  if (pResultBuf->pFree == NULL) {
    return pResultBuf->nextPos;
  } else {
    int32_t offset = -1;

    size_t num = taosArrayGetSize(pResultBuf->pFree);
    for(int32_t i = 0; i < num; ++i) {
      SFreeListItem* pi = (SFreeListItem*)taosArrayGet(pResultBuf->pFree, i);
      if (pi->len >= size) {
        offset = pi->offset;
        pi->offset += (int32_t)size;
        pi->len -= (int32_t)size;

        return offset;
      }
    }

    // no available recycle space, allocate new area in file
    return pResultBuf->nextPos;
  }
}

static char* doFlushPageToDisk(SDiskbasedResultBuf* pResultBuf, SPageInfo* pg) {
  assert(!pg->used && pg->pData != NULL);

  int32_t size = -1;
  char* t = doCompressData(GET_DATA_PAYLOAD(pg), pResultBuf->pageSize, &size, pResultBuf);

  // this page is flushed to disk for the first time
  if (pg->info.offset == -1) {
    pg->info.offset = allocatePositionInFile(pResultBuf, size);
    pResultBuf->nextPos += size;

    int32_t ret = fseek(pResultBuf->file, pg->info.offset, SEEK_SET);
    assert(ret == 0);

    ret = (int32_t) fwrite(t, 1, size, pResultBuf->file);
    assert(ret == size);

    if (pResultBuf->fileSize < pg->info.offset + pg->info.length) {
      pResultBuf->fileSize = pg->info.offset + pg->info.length;
    }
  } else {
    // length becomes greater, current space is not enough, allocate new place, otherwise, do nothing
    if (pg->info.length < size) {
      // 1. add current space to free list
      taosArrayPush(pResultBuf->pFree, &pg->info);

      // 2. allocate new position, and update the info
      pg->info.offset = allocatePositionInFile(pResultBuf, size);
      pResultBuf->nextPos += size;
    }

    //3. write to disk.
    int32_t ret = fseek(pResultBuf->file, pg->info.offset, SEEK_SET);
    if (ret != 0) {  // todo handle the error case

    }

    ret = (int32_t)fwrite(t, size, 1, pResultBuf->file);
    if (ret != size) {  // todo handle the error case

    }

    if (pResultBuf->fileSize < pg->info.offset + pg->info.length) {
      pResultBuf->fileSize = pg->info.offset + pg->info.length;
    }
  }

  char* ret = (char*)pg->pData;
  memset(ret, 0, pResultBuf->pageSize);

  pg->pData = NULL;
  pg->info.length = size;

  pResultBuf->statis.flushBytes += pg->info.length;

  return ret;
}

static char* flushPageToDisk(SDiskbasedResultBuf* pResultBuf, SPageInfo* pg) {
  int32_t ret = TSDB_CODE_SUCCESS;
  assert(((int64_t) pResultBuf->numOfPages * pResultBuf->pageSize) == pResultBuf->totalBufSize && pResultBuf->numOfPages >= pResultBuf->inMemPages);

  if (pResultBuf->file == NULL) {
    if ((ret = createDiskFile(pResultBuf)) != TSDB_CODE_SUCCESS) {
      terrno = ret;
      return NULL;
    }
  }

  return doFlushPageToDisk(pResultBuf, pg);
}

// load file block data in disk
static char* loadPageFromDisk(SDiskbasedResultBuf* pResultBuf, SPageInfo* pg) {
  int32_t ret = fseek(pResultBuf->file, pg->info.offset, SEEK_SET);
  ret = (int32_t)fread(GET_DATA_PAYLOAD(pg), 1, pg->info.length, pResultBuf->file);
  if (ret != pg->info.length) {
    terrno = errno;
    return NULL;
  }

  pResultBuf->statis.loadBytes += pg->info.length;

  int32_t fullSize = 0;
  doDecompressData(GET_DATA_PAYLOAD(pg), pg->info.length, &fullSize, pResultBuf);

  return (char*)GET_DATA_PAYLOAD(pg);
}

static void registerPage(SDiskbasedResultBuf* pResultBuf, int32_t groupId, int32_t pageId) {
  auto& list = pResultBuf->groupSet[groupId];

  pResultBuf->numOfPages += 1;

  SPageInfo ppi;  //{ .info = PAGE_INFO_INITIALIZER, .pageId = pageId, .pn = NULL};

  ppi.pageId = pageId;
  ppi.pData  = NULL;
  ppi.info   = PAGE_INFO_INITIALIZER;
  ppi.used   = true;
  ppi.pn     = NULL;

  list.push_back(ppi);
}

static SListNode* getEldestUnrefedPage(SDiskbasedResultBuf* pResultBuf) {
  SListIter iter = {0};
  tdListInitIter(pResultBuf->lruList, &iter, TD_LIST_BACKWARD);

  SListNode* pn = NULL;
  while((pn = tdListNext(&iter)) != NULL) {
    assert(pn != NULL);

    SPageInfo* pageInfo = *(SPageInfo**) pn->data;
    assert(pageInfo->pageId >= 0 && pageInfo->pn == pn);

    if (!pageInfo->used) {
      break;
    }
  }

  return pn;
}

static char* evicOneDataPage(SDiskbasedResultBuf* pResultBuf) {
  char* bufPage = NULL;
  SListNode* pn = getEldestUnrefedPage(pResultBuf);

  // all pages are referenced by user, try to allocate new space
  if (pn == NULL) {
    int32_t prev = pResultBuf->inMemPages;

    // increase by 50% of previous mem pages
    pResultBuf->inMemPages = (int32_t)(pResultBuf->inMemPages * 1.5f);

    qWarn("%p in memory buf page not sufficient, expand from %d to %d, page size:%d", pResultBuf, prev,
          pResultBuf->inMemPages, pResultBuf->pageSize);
  } else {
    pResultBuf->statis.flushPages += 1;
    tdListPopNode(pResultBuf->lruList, pn);

    SPageInfo* d = *(SPageInfo**) pn->data;
    assert(d->pn == pn);

    d->pn = NULL;
    tfree(pn);

    bufPage = flushPageToDisk(pResultBuf, d);
  }

  return bufPage;
}

static void lruListPushFront(SList *pList, SPageInfo* pi) {
  tdListPrepend(pList, &pi);
  SListNode* front = tdListGetHead(pList);
  pi->pn = front;
}

static void lruListMoveToFront(SList *pList, SPageInfo* pi) {
  tdListPopNode(pList, pi->pn);
  tdListPrependNode(pList, pi->pn);
}

tFilePage* getNewDataBuf(SDiskbasedResultBuf* pResultBuf, int32_t groupId, int32_t* pageId) {
  pResultBuf->statis.getPages += 1;

  char* availablePage = NULL;
  if (NO_IN_MEM_AVAILABLE_PAGES(pResultBuf)) {
    availablePage = evicOneDataPage(pResultBuf);
  }

  // register new id in this group
  *pageId = (++pResultBuf->allocateId);

  // register page id info
  registerPage(pResultBuf, groupId, *pageId);
  SPageInfo& pi = pResultBuf->groupSet[groupId].back();

  // add to LRU list
  assert(listNEles(pResultBuf->lruList) < pResultBuf->inMemPages && pResultBuf->inMemPages > 0);

  lruListPushFront(pResultBuf->lruList, &pi);

  // add to hash map
  taosHashPut(pResultBuf->all, pageId, sizeof(int32_t), &pi, POINTER_BYTES);

  // allocate buf
  if (availablePage == NULL) {
    pi.pData = calloc(1, pResultBuf->pageSize + POINTER_BYTES + 2);  // add extract bytes in case of zipped buffer increased.
  } else {
    pi.pData = availablePage;
  }

  pResultBuf->totalBufSize += pResultBuf->pageSize;

  ((void**)pi.pData)[0] = &pi;
  pi.used = true;

  return (tFilePage*)(GET_DATA_PAYLOAD(&pi));
}

tFilePage* getResBufPage(SDiskbasedResultBuf* pResultBuf, int32_t id) {
  assert(pResultBuf != NULL && id >= 0);
  pResultBuf->statis.getPages += 1;

  SPageInfo** pi = (SPageInfo**)taosHashGet(pResultBuf->all, &id, sizeof(int32_t));
  assert(pi != NULL && *pi != NULL);

  if ((*pi)->pData != NULL) { // it is in memory
    // no need to update the LRU list if only one page exists
    if (pResultBuf->numOfPages == 1) {
      (*pi)->used = true;
      return (tFilePage *)(GET_DATA_PAYLOAD(*pi));
    }

    SPageInfo** pInfo = (SPageInfo**) ((*pi)->pn->data);
    assert(*pInfo == *pi);

    lruListMoveToFront(pResultBuf->lruList, (*pi));
    (*pi)->used = true;

    return (tFilePage*)(GET_DATA_PAYLOAD(*pi));

  } else { // not in memory
    assert((*pi)->pData == NULL && (*pi)->pn == NULL && (*pi)->info.length >= 0 && (*pi)->info.offset >= 0);

    char* availablePage = NULL;
    if (NO_IN_MEM_AVAILABLE_PAGES(pResultBuf)) {
      availablePage = evicOneDataPage(pResultBuf);
    }

    if (availablePage == NULL) {
      (*pi)->pData = calloc(1, pResultBuf->pageSize + POINTER_BYTES);
    } else {
      (*pi)->pData = availablePage;
    }

    ((void**)((*pi)->pData))[0] = (*pi);

    lruListPushFront(pResultBuf->lruList, *pi);
    (*pi)->used = true;

    loadPageFromDisk(pResultBuf, *pi);
    return (tFilePage*)(GET_DATA_PAYLOAD(*pi));
  }
}

void releaseResBufPage(SDiskbasedResultBuf* pResultBuf, void* page) {
  assert(pResultBuf != NULL && page != NULL);
  char* p = (char*) page - POINTER_BYTES;

  SPageInfo* ppi = ((SPageInfo**) p)[0];
  releaseResBufPageInfo(pResultBuf, *ppi);
}

void releaseResBufPageInfo(SDiskbasedResultBuf* pResultBuf, SPageInfo &pi) {
  assert(pi.pData != NULL && pi.used);

  pi.used = false;
  pResultBuf->statis.releasePages += 1;
}

size_t getNumOfRowsPerPage(const SDiskbasedResultBuf* pResultBuf) { return pResultBuf->numOfRowsPerPage; }

size_t getNumOfResultBufGroupId(const SDiskbasedResultBuf* pResultBuf) { return pResultBuf->groupSet.size(); }

size_t getResBufSize(const SDiskbasedResultBuf* pResultBuf) { return (size_t)pResultBuf->totalBufSize; }

SPageInfo::~SPageInfo() 
{
  tfree(pData);
}

SDiskbasedResultBuf::~SDiskbasedResultBuf() {
  if (file != NULL) {
    qDebug("QInfo:%p res output buffer closed, total:%.2f Kb, inmem size:%.2f Kb, file size:%.2f Kb",
        handle, totalBufSize/1024.0, listNEles(lruList) * pageSize / 1024.0,
        fileSize/1024.0);

    fclose(file);
  } else {
    qDebug("QInfo:%p res output buffer closed, total:%.2f Kb, no file created", handle,
           totalBufSize/1024.0);
  }

  unlink(path.c_str());

  tdListFree(lruList);
  taosArrayDestroy((SArray*)emptyDummyIdList);
  taosHashCleanup(all);

  tfree(assistBuf);
}