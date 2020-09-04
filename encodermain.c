/*
 ============================================================================
 Name        : encoder.c
 Author      : 
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include <stdio.h>
#include <stdlib.h>

#include "mysql.h"
#include <pthread.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>

#include <errno.h>
#include <memory.h>
#include <string.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <signal.h>
#include <dirent.h>
#include "encoder.h"




//#include "encoder.c"

//#define DATABASE_IP "192.168.1.30"
#define MIMA "pyt999"
#define DATABASE_IP "127.0.0.1"
//#define MIMA "root"

int Sig_Term=0;
MYSQL   mysql;
MYSQL   mysql1;
char pathdir[100]="/files/TXYS_ZHAOYAN/";
const int YSFW_YY_ID=0x0C;
int preoff=0;
const int GB_ID=0xFF;//广播ID
int xt_cnt=0;
int proc_ing=0;//是否正在执行压缩
//struct SSYC{
//    char zllx;//指令类型
//    char zlbh;//指令编号
//    int zxjg; //执行结果
//    int exptime;//相机曝光时间
//    int frmfrq;//帧频
//    int gain;//增益
//    int yst;
//    int slt;
//    int img_num;
//    int errcode;
//}sssyc;
struct SSYC{
    unsigned char zllx;//指令类型
    unsigned char zlbh;//指令编号
    char zxjg; //执行结果
    char errcode;//错误码
}__attribute__((packed, aligned(1)))sssyc;

struct SSYC_XT{
    unsigned char xt_30;//指令类型
}__attribute__((packed, aligned(1)))sssyc_xt;


void insert_ssyc(char* tmp,int index)
{
	char sql[200];
	int flag;
	char *end;//tmp[200],
//	int index=sizeof(sssyc);
//	memcpy(tmp,&sssyc,sizeof(sssyc));
	sprintf(sql,"insert into GJFW_YC_SSYCSJ(YY_ID,SSYC_CD,SSYC_NR,SSYC_GS) values(%d,%d,",YSFW_YY_ID,index);

	end=sql+strlen(sql);
	*end++ ='\'';
	end+= mysql_real_escape_string(&mysql,end,(char *)tmp,index);
	*end++ ='\'';
	*end++ =',';
	*end++ ='2';
	*end++ =')';

	flag=mysql_real_query(&mysql,sql,(unsigned int)(end-sql));
	if(flag){
		printf("插入实时遥测状态信息错误:%d from %s\n",mysql_error(&mysql),mysql_error(&mysql));
	}
	else
		printf("插入成功\n");
}
void insert_ssyc_xt(char* tmp,int index)
{
	char sql[200];
	int flag;
	char *end;//tmp[200],
//	int index=sizeof(sssyc);
//	memcpy(tmp,&sssyc,sizeof(sssyc));
	sprintf(sql,"insert into GJFW_YC_SSYCSJ(YY_ID,SSYC_CD,SSYC_NR,SSYC_GS) values(%d,%d,",12,index);

	end=sql+strlen(sql);
	*end++ ='\'';
	end+= mysql_real_escape_string(&mysql,end,(char *)tmp,index);
	*end++ ='\'';
	*end++ =',';
	*end++ ='3';
	*end++ =')';

	flag=mysql_real_query(&mysql,sql,(unsigned int)(end-sql));
	if(flag){
		printf("插入实时遥测状态信息错误:%d from %s\n",mysql_error(&mysql),mysql_error(&mysql));
	}
	else
		printf("插入成功\n");
}
void mysql_proc(char *sql,MYSQL *pmysql)
{
	int flag;
	flag=mysql_real_query(pmysql,sql,(unsigned int)strlen(sql));
	if(flag)
	{
		printf("update data error:%d from %s\n",mysql_error(pmysql),mysql_error(pmysql));
		return;
	}
}
int EmptyDir(char *destDir)
{
	DIR *dp;
	struct dirent *entry;
	struct stat statbuf;
	if((dp=opendir(destDir))==NULL)
	{
		fprintf(stderr,"cannot open directory:%s\n",destDir);
		return -1;
	}
	chdir(destDir);
	while((entry==readdir(dp))!=NULL)
	{
		stat(entry->d_name,&statbuf);
		if(S_ISREG(statbuf.st_mode))
		{
			remove(entry->d_name);
		}
	}
	return 0;
}
int connectDB(MYSQL *mysql){

	MYSQL *sock;
	MYSQL_RES *res;//查询结果集，结构类型
	MYSQL_ROW row;//存放一行查询结果的字符串数组
	char qbuf[160];//存放查询sql语句字符串

	if (NULL == mysql_init(mysql)) {    //分配和初始化MYSQL对象
        printf("mysql_init(): %s\n", mysql_error(mysql));
        return -1;
    }

    //尝试与运行在主机上的MySQL数据库引擎建立连接
    if (NULL == mysql_real_connect(mysql,
			DATABASE_IP,//localhost
            "root",
            MIMA,//root"pyt999"
            "node",
            0,
            NULL,
            0)) {
        printf("mysql_real_connect(): %s\n", mysql_error(mysql));
        return -1;
    }
    printf("Connected MySQL successful! \n");
    return 0;
}
void ImageProc(MYSQL_RES *res)
{
	MYSQL_ROW row=NULL;
	int count=0;
	int count_ok=0;
	int count_fail=0;
	char filepath[100];
	char sql[200];
    proc_ing=1;
	while((row=mysql_fetch_row(res)))
	{

		if(count==0)
		{
			//新建文件夹用于存放图像
			//获取当前时间
			time_t nowtime;
			struct tm *timeinfo;
			time(&nowtime);
			timeinfo=localtime(&nowtime);
			int year,month,day,hour,minute;
			year=timeinfo->tm_year+1900;
			month=timeinfo->tm_mon+1;
			day=timeinfo->tm_mday;
			hour=timeinfo->tm_hour;
			minute=timeinfo->tm_min;

			char foldname[100];

			sprintf(foldname,"%04d%02d%02d%02d%02d",year,month,day,hour,minute);
			memcpy(filepath,pathdir,100);
			strcat(filepath,foldname);

			if(access(filepath,0)==-1)
			{
				int kk=mkdir(filepath,0777);
				if(kk)
					printf("create file bag failed\n");
				else
					printf("create file bag success\n");
			}
		}
		int yst_wjid;
		char pathin[100],pathout[100];
		int headsize;
		int imgrow,imgcol,depth;
		char para[2];
		unsigned char yy_id;

		yst_wjid=atoi(row[0]);
		yy_id=atoi(row[2]);
		strcpy(pathin,row[3]);

		headsize=atoi(row[4]);
		imgcol=atoi(row[5]);
		imgrow=atoi(row[6]);
		depth=atoi(row[7]);


		unsigned char yst_para;
		yst_para=atoi(row[8]);


		sprintf(pathout, "%s/%02dyst%08d.raw", filepath, yy_id, count);

		FILE *fp_in;
		fp_in = fopen(pathin, "rb");
		if (fp_in == NULL) {
			printf("can not read infile\n");
			//更新库中执行结果
			sprintf(sql,"update GJFW_YST set YST_ZXJG=-1 where YST_WJID=%d",yst_wjid);
			mysql_proc(sql,&mysql1);
			count_fail++;
			count++;
			continue;
		}

		FILE *fp_out;
		fp_out = fopen(pathout, "wb");
		if (fp_out == NULL) {
			printf("can not create out file\n");
			//更新库中执行结果
			sprintf(sql,"update GJFW_YST set YST_ZXJG=-1 where YST_WJID=%d",yst_wjid);
			mysql_proc(sql,&mysql1);
			count_fail++;
			count++;
			continue;
		}


		unsigned char *head_buffer = (unsigned char *) malloc(headsize);
		fread(head_buffer, sizeof(char), headsize, fp_in);


		//如果压缩参数是200，则使用bz2压缩办法压缩
		//否则使用jpegls压缩方法
		if(yst_para==200)
		{
			//把除文件头以外的部分存为文件temp_bz2.raw
			FILE * fp_bz2;
			fp_bz2=fopen("temp_bz2.raw","wb");

			int filelength =imgcol*imgrow*depth/8;

			unsigned char *filebuf=(unsigned char *)malloc(filelength);
			fread(filebuf,sizeof(char),filelength,fp_in);
			fwrite(filebuf,sizeof(char),filelength,fp_bz2);
			fclose(fp_in);
			fclose(fp_bz2);
			free(filebuf);

			system("tar czf ys_temp_bz2.raw temp_bz2.raw");

			fp_bz2=fopen("ys_temp_bz2.raw","rb");
			fseek(fp_bz2, 0L, SEEK_END);
			int filesize = ftell(fp_bz2);
			fseek(fp_bz2, 0L, SEEK_SET);
			unsigned char *filebuf_ys = (unsigned char *) malloc(filesize);
			fread(filebuf_ys, sizeof(char), filesize, fp_bz2);

			filesize = filesize + 240;

			head_buffer[20] = 0x07;

			head_buffer[21] = filesize / (256 * 256 * 256);
			head_buffer[22] = filesize % (256 * 256 * 256) / (256 * 256);
			head_buffer[23] = filesize % (256 * 256) / 256;
			head_buffer[24] = filesize % 256;
			head_buffer[25] = 0;
			head_buffer[26] = 0xF0; //240
			head_buffer[208] = yst_para;
			head_buffer[78] = 0;
			head_buffer[79] = 0;

			//校验码
			unsigned short crc16val;
			crc16val = crc16(0, head_buffer, 240);
			crc16val = crc16(crc16val, filebuf_ys, filesize - 240);
			head_buffer[78] = crc16val % (256 * 256) / 256;
			head_buffer[79] = crc16val % 256;

			fwrite(head_buffer, sizeof(char), 240, fp_out);
			fwrite(filebuf_ys, sizeof(char), filesize - 240, fp_out);
			free(filebuf_ys);
			free(head_buffer);
			fclose(fp_bz2);
			fclose(fp_out);
			count_ok++;
		}
		else
		{
			//12位图像转16位图像,存为temp_16.raw
			FILE *fp_inout_16;
			fp_inout_16 = fopen("temp_16.raw", "wb");
			int i, j, k;
			int linebyte = imgcol * 12 / 8;
			unsigned short *line = (unsigned short *) malloc(
					imgcol * sizeof(unsigned short));
			unsigned char *linebuf = (unsigned char *) malloc(
					linebyte * sizeof(unsigned char));
			for (i = 0; i < imgrow; i++) {
				k = 0;
				fread(linebuf, sizeof(char), linebyte, fp_in);
				for (j = 0; j < linebyte; j = j + 3) {
					line[k] = linebuf[j] * 16 + (linebuf[j + 1] >> 16);
					line[k + 1] = (linebuf[j + 1] & 0x0F) * 256
							+ linebuf[j + 2];
					k = k + 2;
				}
				fwrite(line, sizeof(short), imgcol, fp_inout_16);
			}
			free(line);
			free(linebuf);
			fclose(fp_inout_16);

			int argc = 2;
			char argv[2][20] = { "-itemp_16.raw", "-otemp.raw" };


			//给压缩模块的输入是temp_16.raw,压缩模块输出是temp.raw
			int err=encode(argc, argv, yst_para, imgcol, imgrow, 16);
			if(err!=0)
			{
				count_fail++;
				printf("fail %d\n",count_fail);
				continue;
			}
			FILE *fp_temp = fopen("temp.raw", "rb");
			fseek(fp_temp, 0L, SEEK_END);
			int filesize = ftell(fp_temp);
			fseek(fp_temp, 0L, SEEK_SET);
			unsigned char *filebuf = (unsigned char *) malloc(filesize);
			fread(filebuf, sizeof(char), filesize, fp_temp);

			filesize = filesize + 240;

			head_buffer[20] = 0x07;

			head_buffer[21] = filesize / (256 * 256 * 256);
			head_buffer[22] = filesize % (256 * 256 * 256) / (256 * 256);
			head_buffer[23] = filesize % (256 * 256) / 256;
			head_buffer[24] = filesize % 256;
			head_buffer[25] = 0;
			head_buffer[26] = 0xF0; //240
			head_buffer[208] = yst_para;
			head_buffer[78] = 0;
			head_buffer[79] = 0;

			//校验码
			unsigned short crc16val;
			crc16val = crc16(0, head_buffer, 240);
			crc16val = crc16(crc16val, filebuf, filesize - 240);
			head_buffer[78] = crc16val % (256 * 256) / 256;
			head_buffer[79] = crc16val % 256;

			fwrite(head_buffer, sizeof(char), 240, fp_out);
			fwrite(filebuf, sizeof(char), filesize - 240, fp_out);
			free(filebuf);

			fclose(fp_in);
			fclose(fp_temp);
			fclose(fp_out);
			free(head_buffer);
			count_ok++;
		}
			//更新库中执行结果

			sprintf(sql,"update GJFW_YST set YST_ZXJG=1,YST_WJLJ=\"%s\" where YST_WJID=%d;",pathout, yst_wjid);
			mysql_proc(sql,&mysql1);
			    count++;
	}
	sssyc.zllx=4;
	sssyc.zlbh=4;
	sssyc.zxjg=2;
	sssyc.errcode=count_ok;
	char tmp[200];
	int index=sizeof(sssyc);
	memcpy(tmp,&sssyc,sizeof(sssyc));
	insert_ssyc(tmp,index);

	sssyc.zllx=4;
	sssyc.zlbh=4;
	sssyc.zxjg=-1;
	sssyc.errcode=count_fail;
	index=sizeof(sssyc);
	memcpy(tmp,&sssyc,sizeof(sssyc));
	insert_ssyc(tmp,index);
	mysql_free_result(res);
	proc_ing=0;
	return;
}
int compressimg()
{
	char sql[200];
	int flag;
	int count=0;
	int count_ok=0;
	int count_fail=0;
	char filepath[100];
	MYSQL_RES *res=NULL;
	MYSQL_ROW row=NULL;

	pthread_t ImageProcID;

	 //在数据库中查找所有待压缩的图像
			sprintf(sql,"select * from GJFW_YST where YST_ZXJG=0;");
			flag=mysql_real_query(&mysql,sql,(unsigned int)strlen(sql));
			if(flag){
				printf("query error:%d from %s\n",mysql_error(&mysql),mysql_error(&mysql));
				return -1;
			}
			//将查询结果读取到内存
			res=mysql_store_result(&mysql);
			if(res->row_count==0)
				return 0;
			else
				pthread_create(&ImageProcID,NULL,(void *)ImageProc,res);


			return 0;
}
int ZLRead(void)
{
	int flag;
	const char sql[200];
	MYSQL_RES *res=NULL;
	MYSQL_ROW row=NULL;
	int near;

    //在数据库中查找所有执行结果为未读，应用ID为载荷相机，广播的记录
	sprintf(sql,"select * from GJFW_YK_ZL where (YY_ID =%d or YY_ID=%d) and ZL_ZXJG=0",YSFW_YY_ID,GB_ID);
	flag=mysql_real_query(&mysql,sql,(unsigned int)strlen(sql));//
	if(flag){
		printf("query error:%d from %s\n",mysql_error(&mysql),mysql_error(&mysql));
		return -1;
	}
	//将查询结果读取到内存
	res=mysql_store_result(&mysql);


	while((row=mysql_fetch_row(res)))
	{

			//对每一行做处理 row[0],row[1]
			int zl_id=atoi(row[0]);
			int zl_lx=atoi(row[2]);
			int zl_bh=atoi(row[3]);
	      if(zl_lx==0x02 && zl_bh==0x07)
			{
				//处理清理数据指令
				char cpara;
				int zxjg_flag=2;


				//remove
				int a =system("rm -rf /files/TXYS_ZHAOYAN/*");//清理文件夹下的图像文件

				sprintf(sql,"truncate GJFW_YST");        //清理数据库GJFW_LRXJ_ZP
				mysql_proc(sql,&mysql);

			    sprintf(sql,"update GJFW_YK_ZL set ZL_ZXJG=%d where ZL_ID=%d",zxjg_flag,zl_id);
                mysql_proc(sql,&mysql);

				//更新关键服务遥控指令表中执行结果
				sssyc.zllx=2;
				sssyc.zlbh=7;
				sssyc.zxjg=2;
				//更新关键服务遥控指令表中执行结果
				char tmp[200];
			    int index=sizeof(sssyc);
			    memcpy(tmp,&sssyc,sizeof(sssyc));
			    insert_ssyc(tmp,index);

			}
			else if(zl_lx==0x02 && zl_bh==0x08)
			{
				//处理预关机指令
				preoff=1;
			    //sprintf(sql,"update GJFW_YK_ZL set ZL_ZXJG=%d where ZL_ID=%d",2,zl_id);
			   // mysql_proc(sql);
				sssyc.zllx=2;
				sssyc.zlbh=8;
				sssyc.zxjg=2;
				//更新关键服务遥控指令表中执行结果
				char tmp[200];
			    int index=sizeof(sssyc);
			    memcpy(tmp,&sssyc,sizeof(sssyc));
			    insert_ssyc(tmp,index);

			}
			else
			{
			    sprintf(sql,"update GJFW_YK_ZL set ZL_ZXJG=-1 where ZL_ID=%d",zl_id);
			    mysql_proc(sql,&mysql);
			}

	}
    mysql_free_result(res);
    return 0;

}


typedef void(* signal_handler)(int);
void end_program(int signal_num)
{
    Sig_Term=1;
}

int main(void)
{
	printf("yst 1.0\n");
	connectDB(&mysql);//数据库连接1
	connectDB(&mysql1);//数据库连接2
	signal_handler p_signal=end_program;
	signal(SIGTERM,p_signal);
	xt_cnt=0;

	pid_t mypid;
    mypid=getpid();
    char sqlxt[200];
    sprintf(sqlxt,"update GJFW_JKGL_YYZT set YY_PID=%d,YY_YXZT=2 where YY_ID=12",mypid);
    mysql_proc(sqlxt,&mysql);


	//int ret_mkdir=system("mkdir -p /home/apps/12/app/yst");
	int ret_mkdir=mkdir("/files/TXYS_ZHAOYAN",S_IRWXU);
	if((ret_mkdir==-1) && (errno!=EEXIST))
	{
		printf("创建文件夹出错3");
		sssyc.errcode=12;//创建收图像文件夹出错
		char tmp[200];
	    int index=sizeof(sssyc);
	    memcpy(tmp,&sssyc,sizeof(sssyc));
		insert_ssyc(tmp,index);
	}

	sssyc.zllx=0;
	sssyc.zlbh=0;
	sssyc.zxjg=2;
	//更新关键服务遥控指令表中执行结果
	char tmp[200];
    int index=sizeof(sssyc);
    memcpy(tmp,&sssyc,sizeof(sssyc));
    insert_ssyc(tmp,index);

	while(1)
	{
		xt_cnt++;
		if(xt_cnt%5==0) //刷新心跳
		{
		    char sqlxt[200];
		    int flag=-1;
		    sprintf(sqlxt,"update GJFW_JKGL_YYZT set YY_SXXT=%d where YY_ID=12",xt_cnt/5);
            mysql_proc(sqlxt,&mysql);
		}
		if(xt_cnt%30==0)    //刷实时遥测
		{
			sssyc_xt.xt_30=1;
			char tmp[200];
		    int index=sizeof(sssyc_xt);
		    memcpy(tmp,&sssyc_xt,sizeof(sssyc_xt));
			insert_ssyc_xt(tmp,index);
		}
		sleep(1);
		if(preoff==1||(Sig_Term==1))
		{
			char sqlxt[200];
			sprintf(sqlxt,"update GJFW_JKGL_YYZT set YY_YXZT=3 where YY_ID=12;");
			mysql_proc(sqlxt,&mysql);
			break;
		}
		ZLRead();
		if(proc_ing==0)
		{
			compressimg();
		}

	}

	return EXIT_SUCCESS;
}
