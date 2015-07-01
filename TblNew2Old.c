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
 *  �� �� ��: TblDcmsCard.c  
 *  ��    �����º��ĸ����ԭ�еı�ṹ����������ƣ�DCMS���췽��Ϊ���¾ɱ���ӳ��
              ���ȡԭ�з�ʽ�����Ѵﵽ��С�Ķ���Ŀ��
    ��    �ܣ����¾ɱ�ӳ��
 *  �����Ա: sniper
 *  ����ʱ��: 2015/06/26
 *  ��    ע: �˳���������ODS�������(CrdDataLoad.sh)�󣬶Ծɱ�����ӳ��(TblDcmsCard.c)֮ǰ  
********************************************************************************************/

#include "bas/bas_global.h"
#include "txn/TblAtfcrd.h"
#include <sys/time.h>

int  mappingNew2OldInit()
{
	    /* �������ݿ� */
    iResult = dbsDbConnect();
    if (iResult != BAS_DBOK)
    {
        MAP_LOG(BAS_LOGERROR, iResult, 0, "dbsDbConnect Failed!");
        return BAS_FAILURE;
    }

    /* ��ѯ��ǰ�������� */
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
    
        /* �����̺��Ƿ���� */
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

    /* �ѵ�ǰ���̸��µ������ܿر��У�ͬʱ������״̬���Ϊ������ */
    memset(&st_TblBatchsysSel, 0x00, sizeof(st_TblBatchsysSel));
    st_TblBatchsysSel.lOutPid = getpid();
    memcpy(st_TblBatchsysSel.caOutStatus, "1", sizeof(st_TblBatchsysSel.caOutStatus));
	memcpy(st_TblBatchsysSel.caOutPrgnm, "��Ƭ�ɼ���ת����������", sizeof(st_TblBatchsysSel.caOutPrgnm));
    st_TblBatchsysSel.lOutBatch_step = 1;
    iResult = UpdatBatchsys(&st_TblBatchsysSel, &st_TblGlobalSel, 1);
    if (iResult != BAS_DBOK) 
    {
        MAP_LOG(BAS_LOGERROR, iResult, 0, "UpdatBatchsys   Failed!");
        return BAS_FAILURE; 
    }

	/* ���ص����������� */
	iResult = LoadOrgAreaCode(gptelem_tAreaNode, gptelem_tBinoNode);
    if (iResult != BAS_DBOK) 
    {
        MAP_LOG(BAS_LOGERROR, iResult, 0, "LoadOrgAreaCode    Failed!");
        return BAS_FAILURE; 
    }
   
}

int mappingGo(){
	  /****************************
	   * �±�Ծɱ�ATFCRDP��ӳ��
	   ****************************/
	  mapTblAtfcrdp();
	  
	  /****************************
	   * �±�Ծɱ�MCPIFPF��ӳ��
	   ****************************/
	  mapTblMcpifpf();
	  
	  /****************************
	   * �±�Ծɱ�MCCADPF��ӳ��
	   ****************************/
	  mapTblMccadpf();
	  
	  /****************************
	   * ATM��ˮ���ɿ������ṩ
	   ****************************/
	  mapTblTxn();
	  
	  
	  
	  
	   /* ��ȡ���ݴ���״̬��(��Ƭ) */
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
        /* �ÿ�Ƭ����״̬Ϊ������ */
        memset(&st_TblDatastSel, 0x00, sizeof(st_TblDatastSel));
        memcpy(st_TblDatastSel.caOutDataType, "01", sizeof(st_TblDatastSel.caOutDataType));
        memcpy(st_TblDatastSel.caOutOnlineSt, "1", sizeof(st_TblDatastSel.caOutOnlineSt));
        iResult = ChangeDataST(&st_TblDatastSel, &st_TblGlobalSel,1);
        if (iResult != BAS_DBOK)
        {
            MAP_LOG(BAS_LOGERROR, iResult, 0, "ChangeDataST   Failed!");
            return BAS_FAILURE;    
        }
        
        /* ������־�� */
        memcpy(st_MrlogIns.caInLogModule,"2022",sizeof(st_MrlogIns.caInLogModule));
        memcpy(st_MrlogIns.caInMsgLevel,"1",sizeof(st_MrlogIns.caInMsgLevel));
        memset(caTmpBuff,0x00,sizeof(caTmpBuff));
        sprintf(caTmpBuff, "%s %d %d", __FILE__, __LINE__, getpid());
        memcpy(st_MrlogIns.caInMsgSrc,&caTmpBuff,sizeof(st_MrlogIns.caInMsgSrc));
        memcpy(st_MrlogIns.caInMsgText,"��Ƭ�ɼ���ת����ʼ",sizeof(st_MrlogIns.caInMsgText));
        iResult = SecWreMrlog(&st_MrlogIns,&st_TblGlobalSel);
        if (iResult != BAS_DBOK)
        {
            MAP_LOG(BAS_LOGERROR, iResult, 0, "SecWreMrlog   Failed!");
            return BAS_FAILURE;    
        }
        /* ���п�Ƭ��ת����ӳ�� */
        iResult = ExCrd2InCrd(&st_TblGlobalSel);
        if (iResult != BAS_DBOK)
        {  
            /* ������־�� */
            memcpy(st_MrlogIns.caInLogModule,"2022",sizeof(st_MrlogIns.caInLogModule));
            memcpy(st_MrlogIns.caInMsgLevel,"3",sizeof(st_MrlogIns.caInMsgLevel));
            memset(caTmpBuff,0x00,sizeof(caTmpBuff));
            sprintf(caTmpBuff, "%s %d %d", __FILE__, __LINE__, getpid());
            memcpy(st_MrlogIns.caInMsgSrc,&caTmpBuff,sizeof(st_MrlogIns.caInMsgSrc));
            memcpy(st_MrlogIns.caInMsgText,"��Ƭ�ɼ���ת������",sizeof(st_MrlogIns.caInMsgText));
            iResult = SecWreMrlog(&st_MrlogIns,&st_TblGlobalSel);
            if (iResult != BAS_DBOK)
            {
                MAP_LOG(BAS_LOGERROR, iResult, 0, "SecWreMrlog   Failed!");
                return BAS_FAILURE;    
            }
            MAP_LOG(BAS_LOGERROR, iResult, 0, "ExCrd2InCrd   Failed!");
            
            /* ���������ܿر�����״̬ */
            memset(&st_TblBatchsysSel, 0x00, sizeof(st_TblBatchsysSel));
            memcpy(st_TblBatchsysSel.caOutStatus, "2", sizeof(st_TblBatchsysSel.caOutStatus));
			memcpy(st_TblBatchsysSel.caOutPrgnm, "��Ƭ�ɼ���ת������", sizeof(st_TblBatchsysSel.caOutPrgnm));
            st_TblBatchsysSel.lOutBatch_step = 1;
            iResult = UpdatBatchsys(&st_TblBatchsysSel, &st_TblGlobalSel, 3);
            if (iResult != BAS_DBOK) 
            {
                MAP_LOG(BAS_LOGERROR, iResult, 0, "UpdatBatchsys   Failed!");
                return BAS_FAILURE; 
            }
            
            /* �ÿ�Ƭ����״̬Ϊ���� */
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

        /* �Կ�Ƭ�����������ת���õ���������� */
		    iResult = AtfcrdpConAndMapping();
		    if (iResult != BAS_DBOK)
        {
            MAP_LOG(BAS_LOGERROR, iResult, 0, "AtfcrdpConAndMapping   Failed!");
            return BAS_FAILURE;    
        }

        /* ������־�� */
        memcpy(st_MrlogIns.caInLogModule,"2022",sizeof(st_MrlogIns.caInLogModule));
        memcpy(st_MrlogIns.caInMsgLevel,"1",sizeof(st_MrlogIns.caInMsgLevel));
        memset(caTmpBuff,0x00,sizeof(caTmpBuff));
        sprintf(caTmpBuff, "%s %d %d", __FILE__, __LINE__, getpid());
        memcpy(st_MrlogIns.caInMsgSrc,&caTmpBuff,sizeof(st_MrlogIns.caInMsgSrc));
        memcpy(st_MrlogIns.caInMsgText,"��Ƭ�ɼ���ת�����",sizeof(st_MrlogIns.caInMsgText));
        iResult = SecWreMrlog(&st_MrlogIns,&st_TblGlobalSel);
        if (iResult != BAS_DBOK)
        {
            MAP_LOG(BAS_LOGERROR, iResult, 0, "SecWreMrlog   Failed!");
            return BAS_FAILURE;    
        }
       
        /* �ÿ�Ƭ����״̬Ϊ��� */
        memset(&st_TblDatastSel, 0x00, sizeof(st_TblDatastSel));
        memcpy(st_TblDatastSel.caOutDataType, "01", sizeof(st_TblDatastSel.caOutDataType));
        memcpy(st_TblDatastSel.caOutOnlineSt, "2", sizeof(st_TblDatastSel.caOutOnlineSt));
        iResult = ChangeDataST(&st_TblDatastSel, &st_TblGlobalSel,0);
        if (iResult != BAS_DBOK)
        {
            MAP_LOG(BAS_LOGERROR, iResult, 0, "ChangeDataST   Failed!");
            return BAS_FAILURE;    
        }
       
        /* ���������ܿر��������衢����״̬ */
        memset(&st_TblBatchsysSel, 0x00, sizeof(st_TblBatchsysSel));
        memcpy(st_TblBatchsysSel.caOutStatus, "1", sizeof(st_TblBatchsysSel.caOutStatus));
		memcpy(st_TblBatchsysSel.caOutPrgnm, "��Ƭ�ɼ���ת�����", sizeof(st_TblBatchsysSel.caOutPrgnm));
        st_TblBatchsysSel.lOutBatch_step = 2;
        iResult = UpdatBatchsys(&st_TblBatchsysSel, &st_TblGlobalSel, 3);
        if (iResult != BAS_DBOK) 
        {
            MAP_LOG(BAS_LOGERROR, iResult, 0, "UpdatBatchsys   Failed!");
            return BAS_FAILURE; 
        }
    }
  
    /* ��ȡ���ݴ���״̬��(��ˮ) */
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
        
        /* ������־�� */
        memcpy(st_MrlogIns.caInLogModule,"2021",sizeof(st_MrlogIns.caInLogModule));
        memcpy(st_MrlogIns.caInMsgLevel,"1",sizeof(st_MrlogIns.caInMsgLevel));
        memset(caTmpBuff,0x00,sizeof(caTmpBuff));
        sprintf(caTmpBuff, "%s %d %d", __FILE__, __LINE__, getpid());
        memcpy(st_MrlogIns.caInMsgSrc,&caTmpBuff,sizeof(st_MrlogIns.caInMsgSrc));
        memcpy(st_MrlogIns.caInMsgText,"��ˮ�ɼ���ת����ʼ",sizeof(st_MrlogIns.caInMsgText));
        iResult = SecWreMrlog(&st_MrlogIns,&st_TblGlobalSel);
        if (iResult != BAS_DBOK)
        {
            MAP_LOG(BAS_LOGERROR, iResult, 0, "SecWreMrlog   Failed!");
            return BAS_FAILURE;    
        }

		memset(&st_TblBatchsysSel, 0x00, sizeof(st_TblBatchsysSel));
	    st_TblBatchsysSel.lOutPid = getpid();
	    memcpy(st_TblBatchsysSel.caOutStatus, "1", sizeof(st_TblBatchsysSel.caOutStatus));
		memcpy(st_TblBatchsysSel.caOutPrgnm, "��ˮ���ݲɼ���ת����������", sizeof(st_TblBatchsysSel.caOutPrgnm));
	    st_TblBatchsysSel.lOutBatch_step = 2;
	    iResult = UpdatBatchsys(&st_TblBatchsysSel, &st_TblGlobalSel, 1);
	    if (iResult != BAS_DBOK) 
	    {
	        MAP_LOG(BAS_LOGERROR, iResult, 0, "UpdatBatchsys   Failed!");
	        return BAS_FAILURE; 
	    }
        
        /* ������ˮ��ת����ӳ�� */
        iResult = ExTxn2InTxn(&st_TblGlobalSel);
        if (iResult != BAS_DBOK)
        {
            /* ������־�� */
            memcpy(st_MrlogIns.caInLogModule,"2021",sizeof(st_MrlogIns.caInLogModule));
            memcpy(st_MrlogIns.caInMsgLevel,"3",sizeof(st_MrlogIns.caInMsgLevel));
            memset(caTmpBuff,0x00,sizeof(caTmpBuff));
            sprintf(caTmpBuff, "%s %d %d", __FILE__, __LINE__, getpid());
            memcpy(st_MrlogIns.caInMsgSrc,&caTmpBuff,sizeof(st_MrlogIns.caInMsgSrc));
            memcpy(st_MrlogIns.caInMsgText,"��ˮ���ݲɼ���ת������",sizeof(st_MrlogIns.caInMsgText));
            iResult = SecWreMrlog(&st_MrlogIns,&st_TblGlobalSel);
            if (iResult != BAS_DBOK)
            {
                MAP_LOG(BAS_LOGERROR, iResult, 0, "SecWreMrlog   Failed!");
                return BAS_FAILURE;    
            }
            MAP_LOG(BAS_LOGERROR, iResult, 0, "ExCrd2InCrd load  Failed!");
           
            /* ���������ܿر�����״̬ */
            memset(&st_TblBatchsysSel, 0x00, sizeof(st_TblBatchsysSel));
            memcpy(st_TblBatchsysSel.caOutStatus, "2", sizeof(st_TblBatchsysSel.caOutStatus));
			memcpy(st_TblBatchsysSel.caOutPrgnm, "��ˮ���ݲɼ���ת������", sizeof(st_TblBatchsysSel.caOutPrgnm));
            st_TblBatchsysSel.lOutBatch_step = 2;
            iResult = UpdatBatchsys(&st_TblBatchsysSel, &st_TblGlobalSel, 3);
            if (iResult != BAS_DBOK) 
            {
                MAP_LOG(BAS_LOGERROR, iResult, 0, "UpdatBatchsys   Failed!");
                return BAS_FAILURE; 
            }
            
            /* ����ˮ����״̬Ϊ���� */
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
        
        /* ������־�� */
        memcpy(st_MrlogIns.caInLogModule,"2021",sizeof(st_MrlogIns.caInLogModule));
        memcpy(st_MrlogIns.caInMsgLevel,"1",sizeof(st_MrlogIns.caInMsgLevel));
        memset(caTmpBuff,0x00,sizeof(caTmpBuff));
        sprintf(caTmpBuff, "%s %d %d", __FILE__, __LINE__, getpid());
        memcpy(st_MrlogIns.caInMsgSrc,&caTmpBuff,sizeof(st_MrlogIns.caInMsgSrc));
        memcpy(st_MrlogIns.caInMsgText,"��ˮ���ݲɼ���ת�����",sizeof(st_MrlogIns.caInMsgText));
        iResult = SecWreMrlog(&st_MrlogIns,&st_TblGlobalSel);
        if (iResult != BAS_DBOK)
        {
            MAP_LOG(BAS_LOGERROR, iResult, 0, "SecWreMrlog   Failed!");
            return BAS_FAILURE;    
        }
       
        /* ����ˮ����״̬Ϊ��� */
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

	/* ��ȡ���ݴ���״̬��(������Ϣ��) */
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
        
        /* ������־�� */
        memcpy(st_MrlogIns.caInLogModule,"2023",sizeof(st_MrlogIns.caInLogModule));
        memcpy(st_MrlogIns.caInMsgLevel,"1",sizeof(st_MrlogIns.caInMsgLevel));
        memset(caTmpBuff,0x00,sizeof(caTmpBuff));
        sprintf(caTmpBuff, "%s %d %d", __FILE__, __LINE__, getpid());
        memcpy(st_MrlogIns.caInMsgSrc,&caTmpBuff,sizeof(st_MrlogIns.caInMsgSrc));
        memcpy(st_MrlogIns.caInMsgText,"һ��ͨ��Ƭ��Ϣ�ɼ���ת��ת����ʼ",sizeof(st_MrlogIns.caInMsgText));
        iResult = SecWreMrlog(&st_MrlogIns,&st_TblGlobalSel);
        if (iResult != BAS_DBOK)
        {
            MAP_LOG(BAS_LOGERROR, iResult, 0, "SecWreMrlog   Failed!");
            return BAS_FAILURE;    
        }

		memset(&st_TblBatchsysSel, 0x00, sizeof(st_TblBatchsysSel));
	    st_TblBatchsysSel.lOutPid = getpid();
	    memcpy(st_TblBatchsysSel.caOutStatus, "1", sizeof(st_TblBatchsysSel.caOutStatus));
		memcpy(st_TblBatchsysSel.caOutPrgnm, "һ��ͨ��Ƭ��Ϣ�ɼ���ת����������", sizeof(st_TblBatchsysSel.caOutPrgnm));
	    st_TblBatchsysSel.lOutBatch_step = 3;
	    iResult = UpdatBatchsys(&st_TblBatchsysSel, &st_TblGlobalSel, 1);
	    if (iResult != BAS_DBOK) 
	    {
	        MAP_LOG(BAS_LOGERROR, iResult, 0, "UpdatBatchsys   Failed!");
	        return BAS_FAILURE; 
	    }
        
        /* ���п�����Ϣ��ת����ӳ�� */
        iResult = ExMcc2InMcc(&st_TblGlobalSel);
        if (iResult != BAS_DBOK)
        {
            /* ������־�� */
            memcpy(st_MrlogIns.caInLogModule,"2023",sizeof(st_MrlogIns.caInLogModule));
            memcpy(st_MrlogIns.caInMsgLevel,"3",sizeof(st_MrlogIns.caInMsgLevel));
            memset(caTmpBuff,0x00,sizeof(caTmpBuff));
            sprintf(caTmpBuff, "%s %d %d", __FILE__, __LINE__, getpid());
            memcpy(st_MrlogIns.caInMsgSrc,&caTmpBuff,sizeof(st_MrlogIns.caInMsgSrc));
            memcpy(st_MrlogIns.caInMsgText,"һ��ͨ��Ƭ��Ϣ�ɼ���ת������",sizeof(st_MrlogIns.caInMsgText));
            iResult = SecWreMrlog(&st_MrlogIns,&st_TblGlobalSel);
            if (iResult != BAS_DBOK)
            {
                MAP_LOG(BAS_LOGERROR, iResult, 0, "SecWreMrlog   Failed!");
                return BAS_FAILURE;    
            }
            MAP_LOG(BAS_LOGERROR, iResult, 0, "ExCrd2InCrd load  Failed!");
           
            /* ���������ܿر�����״̬ */
            memset(&st_TblBatchsysSel, 0x00, sizeof(st_TblBatchsysSel));
            memcpy(st_TblBatchsysSel.caOutStatus, "2", sizeof(st_TblBatchsysSel.caOutStatus));
			memcpy(st_TblBatchsysSel.caOutPrgnm, "һ��ͨ��Ƭ��Ϣ�ɼ���ת������", sizeof(st_TblBatchsysSel.caOutPrgnm));
            st_TblBatchsysSel.lOutBatch_step = 3;
            iResult = UpdatBatchsys(&st_TblBatchsysSel, &st_TblGlobalSel, 3);
            if (iResult != BAS_DBOK) 
            {
                MAP_LOG(BAS_LOGERROR, iResult, 0, "UpdatBatchsys   Failed!");
                return BAS_FAILURE; 
            }
            
            /* �ÿ�����Ϣ����״̬Ϊ���� */
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
        
        /* ������־�� */
        memcpy(st_MrlogIns.caInLogModule,"2023",sizeof(st_MrlogIns.caInLogModule));
        memcpy(st_MrlogIns.caInMsgLevel,"1",sizeof(st_MrlogIns.caInMsgLevel));
        memset(caTmpBuff,0x00,sizeof(caTmpBuff));
        sprintf(caTmpBuff, "%s %d %d", __FILE__, __LINE__, getpid());
        memcpy(st_MrlogIns.caInMsgSrc,&caTmpBuff,sizeof(st_MrlogIns.caInMsgSrc));
        memcpy(st_MrlogIns.caInMsgText,"һ��ͨ��Ƭ��Ϣ�ɼ���ת�����",sizeof(st_MrlogIns.caInMsgText));
        iResult = SecWreMrlog(&st_MrlogIns,&st_TblGlobalSel);
        if (iResult != BAS_DBOK)
        {
            MAP_LOG(BAS_LOGERROR, iResult, 0, "SecWreMrlog   Failed!");
            return BAS_FAILURE;    
        }            
       
        /* �ÿ�����Ϣ����״̬Ϊ��� */
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

	/* ��ȡ���ݴ���״̬��(�ⲿ�ͻ���Ϣ��) */
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
        
        /* ������־�� */
        memcpy(st_MrlogIns.caInLogModule,"2024",sizeof(st_MrlogIns.caInLogModule));
        memcpy(st_MrlogIns.caInMsgLevel,"1",sizeof(st_MrlogIns.caInMsgLevel));
        memset(caTmpBuff,0x00,sizeof(caTmpBuff));
        sprintf(caTmpBuff, "%s %d %d", __FILE__, __LINE__, getpid());
        memcpy(st_MrlogIns.caInMsgSrc,&caTmpBuff,sizeof(st_MrlogIns.caInMsgSrc));
        memcpy(st_MrlogIns.caInMsgText,"һ��ͨ�ⲿ�ͻ���Ϣ�ɼ���ת����ʼ",sizeof(st_MrlogIns.caInMsgText));
        iResult = SecWreMrlog(&st_MrlogIns,&st_TblGlobalSel);
        if (iResult != BAS_DBOK)
        {
            MAP_LOG(BAS_LOGERROR, iResult, 0, "SecWreMrlog   Failed!");
            return BAS_FAILURE;    
        }

		memset(&st_TblBatchsysSel, 0x00, sizeof(st_TblBatchsysSel));
	    st_TblBatchsysSel.lOutPid = getpid();
	    memcpy(st_TblBatchsysSel.caOutStatus, "1", sizeof(st_TblBatchsysSel.caOutStatus));
		memcpy(st_TblBatchsysSel.caOutPrgnm, "һ��ͨ�ⲿ�ͻ���Ϣ�ɼ���ת����������", sizeof(st_TblBatchsysSel.caOutPrgnm));
	    st_TblBatchsysSel.lOutBatch_step = 4;
	    iResult = UpdatBatchsys(&st_TblBatchsysSel, &st_TblGlobalSel, 1);
	    if (iResult != BAS_DBOK) 
	    {
	        MAP_LOG(BAS_LOGERROR, iResult, 0, "UpdatBatchsys   Failed!");
	        return BAS_FAILURE; 
	    }
        
        /* �����ⲿ�ͻ���Ϣ�ɼ���ת�� */
        iResult = ExMcp2InMcp(&st_TblGlobalSel);
        if (iResult != BAS_DBOK)
        {
            /* ������־�� */
            memcpy(st_MrlogIns.caInLogModule,"2024",sizeof(st_MrlogIns.caInLogModule));
            memcpy(st_MrlogIns.caInMsgLevel,"3",sizeof(st_MrlogIns.caInMsgLevel));
            memset(caTmpBuff,0x00,sizeof(caTmpBuff));
            sprintf(caTmpBuff, "%s %d %d", __FILE__, __LINE__, getpid());
            memcpy(st_MrlogIns.caInMsgSrc,&caTmpBuff,sizeof(st_MrlogIns.caInMsgSrc));
            memcpy(st_MrlogIns.caInMsgText,"һ��ͨ�ⲿ�ͻ���Ϣ�ɼ���ת������",sizeof(st_MrlogIns.caInMsgText));
            iResult = SecWreMrlog(&st_MrlogIns,&st_TblGlobalSel);
            if (iResult != BAS_DBOK)
            {
                MAP_LOG(BAS_LOGERROR, iResult, 0, "SecWreMrlog   Failed!");
                return BAS_FAILURE;    
            }
            MAP_LOG(BAS_LOGERROR, iResult, 0, "ExCrd2InCrd load  Failed!");
           
            /* ���������ܿر�����״̬ */
            memset(&st_TblBatchsysSel, 0x00, sizeof(st_TblBatchsysSel));
            memcpy(st_TblBatchsysSel.caOutStatus, "2", sizeof(st_TblBatchsysSel.caOutStatus));
		    memcpy(st_TblBatchsysSel.caOutPrgnm, "һ��ͨ�ⲿ�ͻ���Ϣ�ɼ���ת������", sizeof(st_TblBatchsysSel.caOutPrgnm));
            st_TblBatchsysSel.lOutBatch_step = 3;
            iResult = UpdatBatchsys(&st_TblBatchsysSel, &st_TblGlobalSel, 3);
            if (iResult != BAS_DBOK) 
            {
                MAP_LOG(BAS_LOGERROR, iResult, 0, "UpdatBatchsys   Failed!");
                return BAS_FAILURE; 
            }
            
            /* ���ⲿ�ͻ���Ϣ״̬Ϊ���� */
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
        
        /* ������־�� */
        memcpy(st_MrlogIns.caInLogModule,"2024",sizeof(st_MrlogIns.caInLogModule));
        memcpy(st_MrlogIns.caInMsgLevel,"1",sizeof(st_MrlogIns.caInMsgLevel));
        memset(caTmpBuff,0x00,sizeof(caTmpBuff));
        sprintf(caTmpBuff, "%s %d %d", __FILE__, __LINE__, getpid());
        memcpy(st_MrlogIns.caInMsgSrc,&caTmpBuff,sizeof(st_MrlogIns.caInMsgSrc));
        memcpy(st_MrlogIns.caInMsgText,"һ��ͨ�ⲿ�ͻ���Ϣ�ɼ���ת�����",sizeof(st_MrlogIns.caInMsgText));
        iResult = SecWreMrlog(&st_MrlogIns,&st_TblGlobalSel);
        if (iResult != BAS_DBOK)
        {
            MAP_LOG(BAS_LOGERROR, iResult, 0, "SecWreMrlog   Failed!");
            return BAS_FAILURE;    
        } 
        
        /* ���������ܿر��������衢����״̬ */
        memset(&st_TblBatchsysSel, 0x00, sizeof(st_TblBatchsysSel));
        memcpy(st_TblBatchsysSel.caOutStatus, "3", sizeof(st_TblBatchsysSel.caOutStatus));
		memcpy(st_TblBatchsysSel.caOutPrgnm, "���ݲɼ�ȫ�����", sizeof(st_TblBatchsysSel.caOutPrgnm));
        st_TblBatchsysSel.lOutBatch_step = 4;
        iResult = UpdatBatchsys(&st_TblBatchsysSel, &st_TblGlobalSel, 2);
        if (iResult != BAS_DBOK) 
        {
            MAP_LOG(BAS_LOGERROR, iResult, 0, "UpdatBatchsys   Failed!");
            return BAS_FAILURE; 
        }        
       
        /* ����ˮ����״̬Ϊ��� */
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
        /* ���������ܿر��������衢����״̬ */
        memset(&st_TblBatchsysSel, 0x00, sizeof(st_TblBatchsysSel));
        memcpy(st_TblBatchsysSel.caOutStatus, "3", sizeof(st_TblBatchsysSel.caOutStatus));
		memcpy(st_TblBatchsysSel.caOutPrgnm, "���ݲɼ�ȫ�����", sizeof(st_TblBatchsysSel.caOutPrgnm));
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
 * ������������ͷ��ڴ������
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

/* for git test */
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

