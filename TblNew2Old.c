/*******************************************************************************************
 *  Copyright 2011, Shanghai Huateng Software Systems Co., Ltd.
 *  All right reserved.
 *
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF SHANGHAI HUATENG
 *  SOFTWARE SYSTEMS CO., LTD.  THE CONTENTS OF THIS FILE MAY NOT
 *  BE DISCLOSED TO THIRD PARTIES, COPIED OR DUPLICATED IN ANY FORM,
 *  IN WHOLE OR IN PART, WITHOUT THE PRIOR WRITTEN PERMISSION OF
 *  SHANGHAI HUATENG SOFTWARE SYSTEMS CO., LTD.
 *
 *  文 件 名: TblDcmsCard.c  
 *  背    景：新核心改造后原有的表结构进行重新设计，DCMS改造方法为将新旧表做映射
              后采取原有方式处理已达到最小改动的目的
    功    能：做新旧表映射
 *  编程人员: sniper
 *  开发时间: 2015/06/26
 *  备    注: 此程序运行在ODS供数入库(CrdDataLoad.sh)后，对旧表数据映射(TblDcmsCard.c)之前  
********************************************************************************************/

#include "bas/bas_global.h"
#include "txn/TblAtfcrd.h"
#include <sys/time.h>

int  mappingNew2OldInit()
{
	    /* 连接数据库 */
    iResult = dbsDbConnect();
    if (iResult != BAS_DBOK)
    {
        MAP_LOG(BAS_LOGERROR, iResult, 0, "dbsDbConnect Failed!");
        return BAS_FAILURE;
    }

    /* 查询当前批量日期 */
    dbsTblGlobalSelInit(&st_TblGlobalSel);
    iResult = dbsTblGlobalSel(&st_TblGlobalSel);
    if (iResult != BAS_DBOK)
    {
        MAP_LOG(BAS_LOGERROR, iResult, 0,"Global select  Failed!");
        return BAS_FAILURE;
    }
    
    if ((pcaAppHome = getenv("MALOGHOME")) == NULL)
    {
        return BAS_FAILURE;
    }

	gptelem_tAreaNode = (elem_tAreaNode*)malloc(sizeof(elem_tAreaNode));
    if(gptelem_tAreaNode == NULL)
    {
        MAP_LOG(BAS_LOGERROR, iResult, 0, "calloc   gptelem_tAreaNode  Failed!");
        return -1;
    }
    gptelem_tAreaNode->next = NULL;

	gptelem_tBinoNode = (elem_tBinoNode*)malloc(sizeof(elem_tBinoNode));
    if(gptelem_tBinoNode == NULL)
    {
        MAP_LOG(BAS_LOGERROR, iResult, 0, "calloc   gptelem_tAreaNode  Failed!");
        return -1;
    }
    gptelem_tBinoNode->next = NULL;

    memset(cFileName, 0x00, sizeof(cFileName));
    sprintf(cFileName,"MABHDATE=%s",st_TblGlobalSel.caOutBhdate);
    putenv(cFileName);
    
        /* 检查进程号是否存在 */
    dbsTblBatchsysSelInit(&st_TblBatchsysSel);
    iResult = dbsTblBatchsysSel(&st_TblBatchsysSel);
    if ((iResult != BAS_DBOK) && (iResult!= BAS_DBNOTFOUND))
    {
        MAP_LOG(BAS_LOGERROR, iResult, 0, "dbsTblBatchsysSel dbfetch error");
        return  BAS_FAILURE;
    }
    ProcId = (int)st_TblBatchsysSel.lOutPid;
    iResult = JudgeProcExist(ProcId);
    if (iResult == 1)
    {
        dbsDbDisconnect();
        return  BAS_FAILURE;
    }

    /* 把当前进程更新到批量总控表中，同时把运行状态变更为运行中 */
    memset(&st_TblBatchsysSel, 0x00, sizeof(st_TblBatchsysSel));
    st_TblBatchsysSel.lOutPid = getpid();
    memcpy(st_TblBatchsysSel.caOutStatus, "1", sizeof(st_TblBatchsysSel.caOutStatus));
	memcpy(st_TblBatchsysSel.caOutPrgnm, "卡片采集和转换正在运行", sizeof(st_TblBatchsysSel.caOutPrgnm));
    st_TblBatchsysSel.lOutBatch_step = 1;
    iResult = UpdatBatchsys(&st_TblBatchsysSel, &st_TblGlobalSel, 1);
    if (iResult != BAS_DBOK) 
    {
        MAP_LOG(BAS_LOGERROR, iResult, 0, "UpdatBatchsys   Failed!");
        return BAS_FAILURE; 
    }

	/* 加载地区机构编码 */
	iResult = LoadOrgAreaCode(gptelem_tAreaNode, gptelem_tBinoNode);
    if (iResult != BAS_DBOK) 
    {
        MAP_LOG(BAS_LOGERROR, iResult, 0, "LoadOrgAreaCode    Failed!");
        return BAS_FAILURE; 
    }
   
}

int mappingGo(){
	  /****************************
	   * 新表对旧表ATFCRDP的映射
	   ****************************/
	  mapTblAtfcrdp();
	  
	  /****************************
	   * 新表对旧表MCPIFPF的映射
	   ****************************/
	  mapTblMcpifpf();
	  
	  /****************************
	   * 新表对旧表MCCADPF的映射
	   ****************************/
	  mapTblMccadpf();
	  
	  /****************************
	   * ATM流水改由卡交换提供
	   ****************************/
	  mapTblTxn();
	  
	  
	  
	  
	   /* 读取数据传输状态表(卡片) */
    dbsTblDatastSelInit(&st_TblDatastSel);
    memcpy(st_TblDatastSel.caOutDataType, "01", sizeof(st_TblDatastSel.caOutDataType));
    iResult = ReadDataST(&st_TblDatastSel, &st_TblGlobalSel);
    if ((iResult != BAS_DBOK) && (iResult != 3) && (iResult != 4) && (iResult != 5) && (iResult != 6))
    {
        MAP_LOG(BAS_LOGERROR, iResult, 0, "ReadDataST   Failed!");
        return BAS_FAILURE; 
    }
    else if ((iResult == 3) || (iResult == 4) || (iResult == 6))
    {   
        /* 置卡片传输状态为传输中 */
        memset(&st_TblDatastSel, 0x00, sizeof(st_TblDatastSel));
        memcpy(st_TblDatastSel.caOutDataType, "01", sizeof(st_TblDatastSel.caOutDataType));
        memcpy(st_TblDatastSel.caOutOnlineSt, "1", sizeof(st_TblDatastSel.caOutOnlineSt));
        iResult = ChangeDataST(&st_TblDatastSel, &st_TblGlobalSel,1);
        if (iResult != BAS_DBOK)
        {
            MAP_LOG(BAS_LOGERROR, iResult, 0, "ChangeDataST   Failed!");
            return BAS_FAILURE;    
        }
        
        /* 插入日志表 */
        memcpy(st_MrlogIns.caInLogModule,"2022",sizeof(st_MrlogIns.caInLogModule));
        memcpy(st_MrlogIns.caInMsgLevel,"1",sizeof(st_MrlogIns.caInMsgLevel));
        memset(caTmpBuff,0x00,sizeof(caTmpBuff));
        sprintf(caTmpBuff, "%s %d %d", __FILE__, __LINE__, getpid());
        memcpy(st_MrlogIns.caInMsgSrc,&caTmpBuff,sizeof(st_MrlogIns.caInMsgSrc));
        memcpy(st_MrlogIns.caInMsgText,"卡片采集和转换开始",sizeof(st_MrlogIns.caInMsgText));
        iResult = SecWreMrlog(&st_MrlogIns,&st_TblGlobalSel);
        if (iResult != BAS_DBOK)
        {
            MAP_LOG(BAS_LOGERROR, iResult, 0, "SecWreMrlog   Failed!");
            return BAS_FAILURE;    
        }
        /* 进行卡片表转换和映射 */
        iResult = ExCrd2InCrd(&st_TblGlobalSel);
        if (iResult != BAS_DBOK)
        {  
            /* 插入日志表 */
            memcpy(st_MrlogIns.caInLogModule,"2022",sizeof(st_MrlogIns.caInLogModule));
            memcpy(st_MrlogIns.caInMsgLevel,"3",sizeof(st_MrlogIns.caInMsgLevel));
            memset(caTmpBuff,0x00,sizeof(caTmpBuff));
            sprintf(caTmpBuff, "%s %d %d", __FILE__, __LINE__, getpid());
            memcpy(st_MrlogIns.caInMsgSrc,&caTmpBuff,sizeof(st_MrlogIns.caInMsgSrc));
            memcpy(st_MrlogIns.caInMsgText,"卡片采集和转换出错",sizeof(st_MrlogIns.caInMsgText));
            iResult = SecWreMrlog(&st_MrlogIns,&st_TblGlobalSel);
            if (iResult != BAS_DBOK)
            {
                MAP_LOG(BAS_LOGERROR, iResult, 0, "SecWreMrlog   Failed!");
                return BAS_FAILURE;    
            }
            MAP_LOG(BAS_LOGERROR, iResult, 0, "ExCrd2InCrd   Failed!");
            
            /* 更新批量总控表运行状态 */
            memset(&st_TblBatchsysSel, 0x00, sizeof(st_TblBatchsysSel));
            memcpy(st_TblBatchsysSel.caOutStatus, "2", sizeof(st_TblBatchsysSel.caOutStatus));
			memcpy(st_TblBatchsysSel.caOutPrgnm, "卡片采集和转换出错", sizeof(st_TblBatchsysSel.caOutPrgnm));
            st_TblBatchsysSel.lOutBatch_step = 1;
            iResult = UpdatBatchsys(&st_TblBatchsysSel, &st_TblGlobalSel, 3);
            if (iResult != BAS_DBOK) 
            {
                MAP_LOG(BAS_LOGERROR, iResult, 0, "UpdatBatchsys   Failed!");
                return BAS_FAILURE; 
            }
            
            /* 置卡片传输状态为出错 */
            memset(&st_TblDatastSel, 0x00, sizeof(st_TblDatastSel));
            memcpy(st_TblDatastSel.caOutDataType, "01", sizeof(st_TblDatastSel.caOutDataType));
            memcpy(st_TblDatastSel.caOutOnlineSt, "3", sizeof(st_TblDatastSel.caOutOnlineSt));
            iResult = ChangeDataST(&st_TblDatastSel, &st_TblGlobalSel,0);
            if (iResult != BAS_DBOK)
            {
                MAP_LOG(BAS_LOGERROR, iResult, 0, "ChangeDataST   Failed!");
                return BAS_FAILURE;    
            }
            return BAS_FAILURE;    
        }

        /* 对卡片行所编码进行转换得到其地区编码 */
		    iResult = AtfcrdpConAndMapping();
		    if (iResult != BAS_DBOK)
        {
            MAP_LOG(BAS_LOGERROR, iResult, 0, "AtfcrdpConAndMapping   Failed!");
            return BAS_FAILURE;    
        }

        /* 插入日志表 */
        memcpy(st_MrlogIns.caInLogModule,"2022",sizeof(st_MrlogIns.caInLogModule));
        memcpy(st_MrlogIns.caInMsgLevel,"1",sizeof(st_MrlogIns.caInMsgLevel));
        memset(caTmpBuff,0x00,sizeof(caTmpBuff));
        sprintf(caTmpBuff, "%s %d %d", __FILE__, __LINE__, getpid());
        memcpy(st_MrlogIns.caInMsgSrc,&caTmpBuff,sizeof(st_MrlogIns.caInMsgSrc));
        memcpy(st_MrlogIns.caInMsgText,"卡片采集和转换完成",sizeof(st_MrlogIns.caInMsgText));
        iResult = SecWreMrlog(&st_MrlogIns,&st_TblGlobalSel);
        if (iResult != BAS_DBOK)
        {
            MAP_LOG(BAS_LOGERROR, iResult, 0, "SecWreMrlog   Failed!");
            return BAS_FAILURE;    
        }
       
        /* 置卡片传输状态为完成 */
        memset(&st_TblDatastSel, 0x00, sizeof(st_TblDatastSel));
        memcpy(st_TblDatastSel.caOutDataType, "01", sizeof(st_TblDatastSel.caOutDataType));
        memcpy(st_TblDatastSel.caOutOnlineSt, "2", sizeof(st_TblDatastSel.caOutOnlineSt));
        iResult = ChangeDataST(&st_TblDatastSel, &st_TblGlobalSel,0);
        if (iResult != BAS_DBOK)
        {
            MAP_LOG(BAS_LOGERROR, iResult, 0, "ChangeDataST   Failed!");
            return BAS_FAILURE;    
        }
       
        /* 更新批量总控表批量步骤、运行状态 */
        memset(&st_TblBatchsysSel, 0x00, sizeof(st_TblBatchsysSel));
        memcpy(st_TblBatchsysSel.caOutStatus, "1", sizeof(st_TblBatchsysSel.caOutStatus));
		memcpy(st_TblBatchsysSel.caOutPrgnm, "卡片采集和转换完成", sizeof(st_TblBatchsysSel.caOutPrgnm));
        st_TblBatchsysSel.lOutBatch_step = 2;
        iResult = UpdatBatchsys(&st_TblBatchsysSel, &st_TblGlobalSel, 3);
        if (iResult != BAS_DBOK) 
        {
            MAP_LOG(BAS_LOGERROR, iResult, 0, "UpdatBatchsys   Failed!");
            return BAS_FAILURE; 
        }
    }
  
    /* 读取数据传输状态表(流水) */
    dbsTblDatastSelInit(&st_TblDatastSel);
    memcpy(st_TblDatastSel.caOutDataType, "02", sizeof(st_TblDatastSel.caOutDataType));
    iResult = ReadDataST(&st_TblDatastSel, &st_TblGlobalSel);
    if ((iResult != BAS_DBOK) && (iResult != 3) && (iResult != 4) && (iResult != 5) && (iResult != 6))
    {
        MAP_LOG(BAS_LOGERROR, iResult, 0, "ReadDataST   Failed!");
        return BAS_FAILURE; 
    }
    else if ((iResult == 3) || (iResult == 4) || (iResult == 6))
    {   
        memset(&st_TblDatastSel, 0x00, sizeof(st_TblDatastSel));
        memcpy(st_TblDatastSel.caOutDataType, "02", sizeof(st_TblDatastSel.caOutDataType));
        memcpy(st_TblDatastSel.caOutOnlineSt, "1", sizeof(st_TblDatastSel.caOutOnlineSt));
        iResult = ChangeDataST(&st_TblDatastSel, &st_TblGlobalSel,1);
        if (iResult != BAS_DBOK)
        {
            MAP_LOG(BAS_LOGERROR, iResult, 0, "ChangeDataST   Failed!");
            return BAS_FAILURE;    
        }
        
        /* 插入日志表 */
        memcpy(st_MrlogIns.caInLogModule,"2021",sizeof(st_MrlogIns.caInLogModule));
        memcpy(st_MrlogIns.caInMsgLevel,"1",sizeof(st_MrlogIns.caInMsgLevel));
        memset(caTmpBuff,0x00,sizeof(caTmpBuff));
        sprintf(caTmpBuff, "%s %d %d", __FILE__, __LINE__, getpid());
        memcpy(st_MrlogIns.caInMsgSrc,&caTmpBuff,sizeof(st_MrlogIns.caInMsgSrc));
        memcpy(st_MrlogIns.caInMsgText,"流水采集和转换开始",sizeof(st_MrlogIns.caInMsgText));
        iResult = SecWreMrlog(&st_MrlogIns,&st_TblGlobalSel);
        if (iResult != BAS_DBOK)
        {
            MAP_LOG(BAS_LOGERROR, iResult, 0, "SecWreMrlog   Failed!");
            return BAS_FAILURE;    
        }

		memset(&st_TblBatchsysSel, 0x00, sizeof(st_TblBatchsysSel));
	    st_TblBatchsysSel.lOutPid = getpid();
	    memcpy(st_TblBatchsysSel.caOutStatus, "1", sizeof(st_TblBatchsysSel.caOutStatus));
		memcpy(st_TblBatchsysSel.caOutPrgnm, "流水数据采集和转换正在运行", sizeof(st_TblBatchsysSel.caOutPrgnm));
	    st_TblBatchsysSel.lOutBatch_step = 2;
	    iResult = UpdatBatchsys(&st_TblBatchsysSel, &st_TblGlobalSel, 1);
	    if (iResult != BAS_DBOK) 
	    {
	        MAP_LOG(BAS_LOGERROR, iResult, 0, "UpdatBatchsys   Failed!");
	        return BAS_FAILURE; 
	    }
        
        /* 进行流水表转换和映射 */
        iResult = ExTxn2InTxn(&st_TblGlobalSel);
        if (iResult != BAS_DBOK)
        {
            /* 插入日志表 */
            memcpy(st_MrlogIns.caInLogModule,"2021",sizeof(st_MrlogIns.caInLogModule));
            memcpy(st_MrlogIns.caInMsgLevel,"3",sizeof(st_MrlogIns.caInMsgLevel));
            memset(caTmpBuff,0x00,sizeof(caTmpBuff));
            sprintf(caTmpBuff, "%s %d %d", __FILE__, __LINE__, getpid());
            memcpy(st_MrlogIns.caInMsgSrc,&caTmpBuff,sizeof(st_MrlogIns.caInMsgSrc));
            memcpy(st_MrlogIns.caInMsgText,"流水数据采集和转换出错",sizeof(st_MrlogIns.caInMsgText));
            iResult = SecWreMrlog(&st_MrlogIns,&st_TblGlobalSel);
            if (iResult != BAS_DBOK)
            {
                MAP_LOG(BAS_LOGERROR, iResult, 0, "SecWreMrlog   Failed!");
                return BAS_FAILURE;    
            }
            MAP_LOG(BAS_LOGERROR, iResult, 0, "ExCrd2InCrd load  Failed!");
           
            /* 更新批量总控表运行状态 */
            memset(&st_TblBatchsysSel, 0x00, sizeof(st_TblBatchsysSel));
            memcpy(st_TblBatchsysSel.caOutStatus, "2", sizeof(st_TblBatchsysSel.caOutStatus));
			memcpy(st_TblBatchsysSel.caOutPrgnm, "流水数据采集和转换出错", sizeof(st_TblBatchsysSel.caOutPrgnm));
            st_TblBatchsysSel.lOutBatch_step = 2;
            iResult = UpdatBatchsys(&st_TblBatchsysSel, &st_TblGlobalSel, 3);
            if (iResult != BAS_DBOK) 
            {
                MAP_LOG(BAS_LOGERROR, iResult, 0, "UpdatBatchsys   Failed!");
                return BAS_FAILURE; 
            }
            
            /* 置流水传输状态为出错 */
            memset(&st_TblDatastSel, 0x00, sizeof(st_TblDatastSel));
            memcpy(st_TblDatastSel.caOutDataType, "02", sizeof(st_TblDatastSel.caOutDataType));
            memcpy(st_TblDatastSel.caOutOnlineSt, "3", sizeof(st_TblDatastSel.caOutOnlineSt));
            iResult = ChangeDataST(&st_TblDatastSel, &st_TblGlobalSel,0);
            if (iResult != BAS_DBOK)
            {
                MAP_LOG(BAS_LOGERROR, iResult, 0, "ChangeDataST   Failed!");
                return BAS_FAILURE;    
            }
            return BAS_FAILURE;    
        }
        
        /* 插入日志表 */
        memcpy(st_MrlogIns.caInLogModule,"2021",sizeof(st_MrlogIns.caInLogModule));
        memcpy(st_MrlogIns.caInMsgLevel,"1",sizeof(st_MrlogIns.caInMsgLevel));
        memset(caTmpBuff,0x00,sizeof(caTmpBuff));
        sprintf(caTmpBuff, "%s %d %d", __FILE__, __LINE__, getpid());
        memcpy(st_MrlogIns.caInMsgSrc,&caTmpBuff,sizeof(st_MrlogIns.caInMsgSrc));
        memcpy(st_MrlogIns.caInMsgText,"流水数据采集和转换完成",sizeof(st_MrlogIns.caInMsgText));
        iResult = SecWreMrlog(&st_MrlogIns,&st_TblGlobalSel);
        if (iResult != BAS_DBOK)
        {
            MAP_LOG(BAS_LOGERROR, iResult, 0, "SecWreMrlog   Failed!");
            return BAS_FAILURE;    
        }
       
        /* 置流水传输状态为完成 */
        memset(&st_TblDatastSel, 0x00, sizeof(st_TblDatastSel));
        memcpy(st_TblDatastSel.caOutDataType, "02", sizeof(st_TblDatastSel.caOutDataType));
        memcpy(st_TblDatastSel.caOutOnlineSt, "2", sizeof(st_TblDatastSel.caOutOnlineSt));
        iResult = ChangeDataST(&st_TblDatastSel, &st_TblGlobalSel,0);
        if (iResult != BAS_DBOK)
        {
            MAP_LOG(BAS_LOGERROR, iResult, 0, "ChangeDataST   Failed!");
            return BAS_FAILURE;    
        }
    }  

	/* 读取数据传输状态表(卡客信息表) */
    dbsTblDatastSelInit(&st_TblDatastSel);
    memcpy(st_TblDatastSel.caOutDataType, "04", sizeof(st_TblDatastSel.caOutDataType));
    iResult = ReadDataST(&st_TblDatastSel, &st_TblGlobalSel);
    if ((iResult != BAS_DBOK) && (iResult != 3) && (iResult != 4) && (iResult != 5) && (iResult != 6))
    {
        MAP_LOG(BAS_LOGERROR, iResult, 0, "ReadDataST   Failed!");
        return BAS_FAILURE; 
    }
    else if ((iResult == 3) || (iResult == 4) || (iResult == 6))
    {   
        memset(&st_TblDatastSel, 0x00, sizeof(st_TblDatastSel));
        memcpy(st_TblDatastSel.caOutDataType, "04", sizeof(st_TblDatastSel.caOutDataType));
        memcpy(st_TblDatastSel.caOutOnlineSt, "1", sizeof(st_TblDatastSel.caOutOnlineSt));
        iResult = ChangeDataST(&st_TblDatastSel, &st_TblGlobalSel,1);
        if (iResult != BAS_DBOK)
        {
            MAP_LOG(BAS_LOGERROR, iResult, 0, "ChangeDataST   Failed!");
            return BAS_FAILURE;    
        }
        
        /* 插入日志表 */
        memcpy(st_MrlogIns.caInLogModule,"2023",sizeof(st_MrlogIns.caInLogModule));
        memcpy(st_MrlogIns.caInMsgLevel,"1",sizeof(st_MrlogIns.caInMsgLevel));
        memset(caTmpBuff,0x00,sizeof(caTmpBuff));
        sprintf(caTmpBuff, "%s %d %d", __FILE__, __LINE__, getpid());
        memcpy(st_MrlogIns.caInMsgSrc,&caTmpBuff,sizeof(st_MrlogIns.caInMsgSrc));
        memcpy(st_MrlogIns.caInMsgText,"一户通卡片信息采集和转换转换开始",sizeof(st_MrlogIns.caInMsgText));
        iResult = SecWreMrlog(&st_MrlogIns,&st_TblGlobalSel);
        if (iResult != BAS_DBOK)
        {
            MAP_LOG(BAS_LOGERROR, iResult, 0, "SecWreMrlog   Failed!");
            return BAS_FAILURE;    
        }

		memset(&st_TblBatchsysSel, 0x00, sizeof(st_TblBatchsysSel));
	    st_TblBatchsysSel.lOutPid = getpid();
	    memcpy(st_TblBatchsysSel.caOutStatus, "1", sizeof(st_TblBatchsysSel.caOutStatus));
		memcpy(st_TblBatchsysSel.caOutPrgnm, "一户通卡片信息采集和转换正在运行", sizeof(st_TblBatchsysSel.caOutPrgnm));
	    st_TblBatchsysSel.lOutBatch_step = 3;
	    iResult = UpdatBatchsys(&st_TblBatchsysSel, &st_TblGlobalSel, 1);
	    if (iResult != BAS_DBOK) 
	    {
	        MAP_LOG(BAS_LOGERROR, iResult, 0, "UpdatBatchsys   Failed!");
	        return BAS_FAILURE; 
	    }
        
        /* 进行卡客信息表转换和映射 */
        iResult = ExMcc2InMcc(&st_TblGlobalSel);
        if (iResult != BAS_DBOK)
        {
            /* 插入日志表 */
            memcpy(st_MrlogIns.caInLogModule,"2023",sizeof(st_MrlogIns.caInLogModule));
            memcpy(st_MrlogIns.caInMsgLevel,"3",sizeof(st_MrlogIns.caInMsgLevel));
            memset(caTmpBuff,0x00,sizeof(caTmpBuff));
            sprintf(caTmpBuff, "%s %d %d", __FILE__, __LINE__, getpid());
            memcpy(st_MrlogIns.caInMsgSrc,&caTmpBuff,sizeof(st_MrlogIns.caInMsgSrc));
            memcpy(st_MrlogIns.caInMsgText,"一户通卡片信息采集和转换出错",sizeof(st_MrlogIns.caInMsgText));
            iResult = SecWreMrlog(&st_MrlogIns,&st_TblGlobalSel);
            if (iResult != BAS_DBOK)
            {
                MAP_LOG(BAS_LOGERROR, iResult, 0, "SecWreMrlog   Failed!");
                return BAS_FAILURE;    
            }
            MAP_LOG(BAS_LOGERROR, iResult, 0, "ExCrd2InCrd load  Failed!");
           
            /* 更新批量总控表运行状态 */
            memset(&st_TblBatchsysSel, 0x00, sizeof(st_TblBatchsysSel));
            memcpy(st_TblBatchsysSel.caOutStatus, "2", sizeof(st_TblBatchsysSel.caOutStatus));
			memcpy(st_TblBatchsysSel.caOutPrgnm, "一户通卡片信息采集和转换出错", sizeof(st_TblBatchsysSel.caOutPrgnm));
            st_TblBatchsysSel.lOutBatch_step = 3;
            iResult = UpdatBatchsys(&st_TblBatchsysSel, &st_TblGlobalSel, 3);
            if (iResult != BAS_DBOK) 
            {
                MAP_LOG(BAS_LOGERROR, iResult, 0, "UpdatBatchsys   Failed!");
                return BAS_FAILURE; 
            }
            
            /* 置卡客信息传输状态为出错 */
            memset(&st_TblDatastSel, 0x00, sizeof(st_TblDatastSel));
            memcpy(st_TblDatastSel.caOutDataType, "04", sizeof(st_TblDatastSel.caOutDataType));
            memcpy(st_TblDatastSel.caOutOnlineSt, "3", sizeof(st_TblDatastSel.caOutOnlineSt));
            iResult = ChangeDataST(&st_TblDatastSel, &st_TblGlobalSel,0);
            if (iResult != BAS_DBOK)
            {
                MAP_LOG(BAS_LOGERROR, iResult, 0, "ChangeDataST   Failed!");
                return BAS_FAILURE;    
            }
            return BAS_FAILURE;    
        }
        
        /* 插入日志表 */
        memcpy(st_MrlogIns.caInLogModule,"2023",sizeof(st_MrlogIns.caInLogModule));
        memcpy(st_MrlogIns.caInMsgLevel,"1",sizeof(st_MrlogIns.caInMsgLevel));
        memset(caTmpBuff,0x00,sizeof(caTmpBuff));
        sprintf(caTmpBuff, "%s %d %d", __FILE__, __LINE__, getpid());
        memcpy(st_MrlogIns.caInMsgSrc,&caTmpBuff,sizeof(st_MrlogIns.caInMsgSrc));
        memcpy(st_MrlogIns.caInMsgText,"一户通卡片信息采集和转换完成",sizeof(st_MrlogIns.caInMsgText));
        iResult = SecWreMrlog(&st_MrlogIns,&st_TblGlobalSel);
        if (iResult != BAS_DBOK)
        {
            MAP_LOG(BAS_LOGERROR, iResult, 0, "SecWreMrlog   Failed!");
            return BAS_FAILURE;    
        }            
       
        /* 置卡客信息传输状态为完成 */
        memset(&st_TblDatastSel, 0x00, sizeof(st_TblDatastSel));
        memcpy(st_TblDatastSel.caOutDataType, "04", sizeof(st_TblDatastSel.caOutDataType));
        memcpy(st_TblDatastSel.caOutOnlineSt, "2", sizeof(st_TblDatastSel.caOutOnlineSt));
        iResult = ChangeDataST(&st_TblDatastSel, &st_TblGlobalSel,0);
        if (iResult != BAS_DBOK)
        {
            MAP_LOG(BAS_LOGERROR, iResult, 0, "ChangeDataST   Failed!");
            return BAS_FAILURE;    
        }
    }  

	/* 读取数据传输状态表(外部客户信息表) */
    dbsTblDatastSelInit(&st_TblDatastSel);
    memcpy(st_TblDatastSel.caOutDataType, "05", sizeof(st_TblDatastSel.caOutDataType));
    iResult = ReadDataST(&st_TblDatastSel, &st_TblGlobalSel);
    if ((iResult != BAS_DBOK) && (iResult != 3) && (iResult != 4) && (iResult != 5) && (iResult != 6))
    {
        MAP_LOG(BAS_LOGERROR, iResult, 0, "ReadDataST   Failed!");
        return BAS_FAILURE; 
    }
    else if ((iResult == 3) || (iResult == 4) || (iResult == 6))
    {   
        memset(&st_TblDatastSel, 0x00, sizeof(st_TblDatastSel));
        memcpy(st_TblDatastSel.caOutDataType, "05", sizeof(st_TblDatastSel.caOutDataType));
        memcpy(st_TblDatastSel.caOutOnlineSt, "1", sizeof(st_TblDatastSel.caOutOnlineSt));
        iResult = ChangeDataST(&st_TblDatastSel, &st_TblGlobalSel,1);
        if (iResult != BAS_DBOK)
        {
            MAP_LOG(BAS_LOGERROR, iResult, 0, "ChangeDataST   Failed!");
            return BAS_FAILURE;    
        }
        
        /* 插入日志表 */
        memcpy(st_MrlogIns.caInLogModule,"2024",sizeof(st_MrlogIns.caInLogModule));
        memcpy(st_MrlogIns.caInMsgLevel,"1",sizeof(st_MrlogIns.caInMsgLevel));
        memset(caTmpBuff,0x00,sizeof(caTmpBuff));
        sprintf(caTmpBuff, "%s %d %d", __FILE__, __LINE__, getpid());
        memcpy(st_MrlogIns.caInMsgSrc,&caTmpBuff,sizeof(st_MrlogIns.caInMsgSrc));
        memcpy(st_MrlogIns.caInMsgText,"一户通外部客户信息采集和转换开始",sizeof(st_MrlogIns.caInMsgText));
        iResult = SecWreMrlog(&st_MrlogIns,&st_TblGlobalSel);
        if (iResult != BAS_DBOK)
        {
            MAP_LOG(BAS_LOGERROR, iResult, 0, "SecWreMrlog   Failed!");
            return BAS_FAILURE;    
        }

		memset(&st_TblBatchsysSel, 0x00, sizeof(st_TblBatchsysSel));
	    st_TblBatchsysSel.lOutPid = getpid();
	    memcpy(st_TblBatchsysSel.caOutStatus, "1", sizeof(st_TblBatchsysSel.caOutStatus));
		memcpy(st_TblBatchsysSel.caOutPrgnm, "一户通外部客户信息采集和转换正在运行", sizeof(st_TblBatchsysSel.caOutPrgnm));
	    st_TblBatchsysSel.lOutBatch_step = 4;
	    iResult = UpdatBatchsys(&st_TblBatchsysSel, &st_TblGlobalSel, 1);
	    if (iResult != BAS_DBOK) 
	    {
	        MAP_LOG(BAS_LOGERROR, iResult, 0, "UpdatBatchsys   Failed!");
	        return BAS_FAILURE; 
	    }
        
        /* 进行外部客户信息采集和转换 */
        iResult = ExMcp2InMcp(&st_TblGlobalSel);
        if (iResult != BAS_DBOK)
        {
            /* 插入日志表 */
            memcpy(st_MrlogIns.caInLogModule,"2024",sizeof(st_MrlogIns.caInLogModule));
            memcpy(st_MrlogIns.caInMsgLevel,"3",sizeof(st_MrlogIns.caInMsgLevel));
            memset(caTmpBuff,0x00,sizeof(caTmpBuff));
            sprintf(caTmpBuff, "%s %d %d", __FILE__, __LINE__, getpid());
            memcpy(st_MrlogIns.caInMsgSrc,&caTmpBuff,sizeof(st_MrlogIns.caInMsgSrc));
            memcpy(st_MrlogIns.caInMsgText,"一户通外部客户信息采集和转换出错",sizeof(st_MrlogIns.caInMsgText));
            iResult = SecWreMrlog(&st_MrlogIns,&st_TblGlobalSel);
            if (iResult != BAS_DBOK)
            {
                MAP_LOG(BAS_LOGERROR, iResult, 0, "SecWreMrlog   Failed!");
                return BAS_FAILURE;    
            }
            MAP_LOG(BAS_LOGERROR, iResult, 0, "ExCrd2InCrd load  Failed!");
           
            /* 更新批量总控表运行状态 */
            memset(&st_TblBatchsysSel, 0x00, sizeof(st_TblBatchsysSel));
            memcpy(st_TblBatchsysSel.caOutStatus, "2", sizeof(st_TblBatchsysSel.caOutStatus));
		    memcpy(st_TblBatchsysSel.caOutPrgnm, "一户通外部客户信息采集和转换出错", sizeof(st_TblBatchsysSel.caOutPrgnm));
            st_TblBatchsysSel.lOutBatch_step = 3;
            iResult = UpdatBatchsys(&st_TblBatchsysSel, &st_TblGlobalSel, 3);
            if (iResult != BAS_DBOK) 
            {
                MAP_LOG(BAS_LOGERROR, iResult, 0, "UpdatBatchsys   Failed!");
                return BAS_FAILURE; 
            }
            
            /* 置外部客户信息状态为出错 */
            memset(&st_TblDatastSel, 0x00, sizeof(st_TblDatastSel));
            memcpy(st_TblDatastSel.caOutDataType, "05", sizeof(st_TblDatastSel.caOutDataType));
            memcpy(st_TblDatastSel.caOutOnlineSt, "3", sizeof(st_TblDatastSel.caOutOnlineSt));
            iResult = ChangeDataST(&st_TblDatastSel, &st_TblGlobalSel,0);
            if (iResult != BAS_DBOK)
            {
                MAP_LOG(BAS_LOGERROR, iResult, 0, "ChangeDataST   Failed!");
                return BAS_FAILURE;    
            }
            return BAS_FAILURE;    
        }
        
        /* 插入日志表 */
        memcpy(st_MrlogIns.caInLogModule,"2024",sizeof(st_MrlogIns.caInLogModule));
        memcpy(st_MrlogIns.caInMsgLevel,"1",sizeof(st_MrlogIns.caInMsgLevel));
        memset(caTmpBuff,0x00,sizeof(caTmpBuff));
        sprintf(caTmpBuff, "%s %d %d", __FILE__, __LINE__, getpid());
        memcpy(st_MrlogIns.caInMsgSrc,&caTmpBuff,sizeof(st_MrlogIns.caInMsgSrc));
        memcpy(st_MrlogIns.caInMsgText,"一户通外部客户信息采集和转换完成",sizeof(st_MrlogIns.caInMsgText));
        iResult = SecWreMrlog(&st_MrlogIns,&st_TblGlobalSel);
        if (iResult != BAS_DBOK)
        {
            MAP_LOG(BAS_LOGERROR, iResult, 0, "SecWreMrlog   Failed!");
            return BAS_FAILURE;    
        } 
        
        /* 更新批量总控表批量步骤、运行状态 */
        memset(&st_TblBatchsysSel, 0x00, sizeof(st_TblBatchsysSel));
        memcpy(st_TblBatchsysSel.caOutStatus, "3", sizeof(st_TblBatchsysSel.caOutStatus));
		memcpy(st_TblBatchsysSel.caOutPrgnm, "数据采集全部完成", sizeof(st_TblBatchsysSel.caOutPrgnm));
        st_TblBatchsysSel.lOutBatch_step = 4;
        iResult = UpdatBatchsys(&st_TblBatchsysSel, &st_TblGlobalSel, 2);
        if (iResult != BAS_DBOK) 
        {
            MAP_LOG(BAS_LOGERROR, iResult, 0, "UpdatBatchsys   Failed!");
            return BAS_FAILURE; 
        }        
       
        /* 置流水传输状态为完成 */
        memset(&st_TblDatastSel, 0x00, sizeof(st_TblDatastSel));
        memcpy(st_TblDatastSel.caOutDataType, "05", sizeof(st_TblDatastSel.caOutDataType));
        memcpy(st_TblDatastSel.caOutOnlineSt, "2", sizeof(st_TblDatastSel.caOutOnlineSt));
        iResult = ChangeDataST(&st_TblDatastSel, &st_TblGlobalSel,0);
        if (iResult != BAS_DBOK)
        {
            MAP_LOG(BAS_LOGERROR, iResult, 0, "ChangeDataST   Failed!");
            return BAS_FAILURE;    
        }
    }  
    else if (iResult == 5)
    {
        /* 更新批量总控表批量步骤、运行状态 */
        memset(&st_TblBatchsysSel, 0x00, sizeof(st_TblBatchsysSel));
        memcpy(st_TblBatchsysSel.caOutStatus, "3", sizeof(st_TblBatchsysSel.caOutStatus));
		memcpy(st_TblBatchsysSel.caOutPrgnm, "数据采集全部完成", sizeof(st_TblBatchsysSel.caOutPrgnm));
        st_TblBatchsysSel.lOutBatch_step = 4;
        iResult = UpdatBatchsys(&st_TblBatchsysSel, &st_TblGlobalSel, 2);
        if (iResult != BAS_DBOK) 
        {
            MAP_LOG(BAS_LOGERROR, iResult, 0, "UpdatBatchsys   Failed!");
            return BAS_FAILURE; 
        }
    }
}

/*******************************
 * 程序结束处理，释放内存和链接
 *******************************/
int mappingEnd(){
	  FreeNode();
	  
    MAP_LOG(MAP_LOGINFO, 0, 0, "Mapping Finish!");
    
    iResult = dbsDbDisconnect();
    if (iResult != BAS_DBOK) 
    {
        MAP_LOG(BAS_LOGERROR, iResult, 0, "dbsDbDisconnect   Failed!");
        return BAS_FAILURE; 
    }
}

int main(int argc, char **argv)
{

    int  iResult;
    int  iCnt;
    int ProcId;
    T_TblGlobalSel  st_TblGlobalSel = {0};
    T_TblDatastSel  st_TblDatastSel = {0};
    T_TblBatchsysSel  st_TblBatchsysSel = {0};
    T_MrlogIns  st_MrlogIns = {0};
    char caTmpBuff[200] = {0};
    char cFileName[20] = {0};
    char *pcaAppHome;
    char *p;
    
    iResult = 0;
    iCnt = 0;
    ProcId = 0;
    
	  
	  mappingNew2OldInit();

    mappingGo();
	
	  mappingEnd();

    return 0;

}

