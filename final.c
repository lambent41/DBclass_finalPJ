#define _CRT_SECURE_NO_WARNINGS
#define BUF_SIZE 1000000
#define DB_NAME "SDS_group4"
#define clear() printf("\033[H\033[J")

#include <mysql.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef enum _boolean {
    FALSE,
    TRUE
} bool;

int nextUserID;
int nextFileID;
int nextSmallID;
int nextBigID;

char dateandtime[20];
char input[10];

//char *server = "localhost";
//char *user = "db_cw";
//char *password = "group4";

char *server = "115.126.220.91";
char *user = "root";
char *password = "01041307689";

int user_id=0;
char user_name[20];
bool select_first=TRUE;

MYSQL *conn;
MYSQL_ROW row;
MYSQL_RES *res;
char *temp_query, query;
bool Query(MYSQL *c, const char *q);	// Error --> return FALSE
void Initialize(MYSQL *conn);
void UserChange(MYSQL *conn);
void Upload(MYSQL *conn);
void Table(MYSQL *conn);
void printTable(MYSQL *conn, char *table);
void Count(MYSQL *conn);
void PrintCount(MYSQL *conn, char *table);
void Update_SB(MYSQL *conn);
void AddUser(MYSQL *conn);
void ListUser(MYSQL *conn);

int main(void){
	FILE *fp;
	int file_size;

	conn = mysql_init(NULL);

	if(!mysql_real_connect(conn, server, user, password, DB_NAME, 0, NULL, 0)){
		fprintf(stderr, "%s\n", mysql_error(conn));
	}

  else {
    printf("\n===================================================================================================\n");
		printf("\nconnection success!\n");
    printf("\n===================================================================================================\n");
	}

	while(1){
		ListUser(conn);
		int select;
		printf("\n===================================================================================================\n");
		printf("0:Select User || 1:Add New User\nType Number(0~1):");
		scanf("%d", &select);
		if(select==0){
			UserChange(conn);
			if(!select_first) break;
		}
		else if(select==1){
			AddUser(conn);
		}
		else{
			printf("\nWrong Number. Type Number(0~1) Again\n");
		}
	}


	while(1){
		printf("\n===================================================================================================\n");
		printf("0:initialize DB|||1:User change|||2:Upload|||3:Table|||4:Count|||5:Update small&big table|||6:CLS\n");
		printf("Put the operating number(exit is only input 'quit') : ");
	  scanf("%s", input);

		if(strcmp(input, "0")==0) { // initialize db
			Initialize(conn);
		}
		else if(strcmp(input, "1")==0) { // user change
			UserChange(conn);
		}
		else if(strcmp(input, "2")==0) { // upload
			Upload(conn);
		}
		else if(strcmp(input, "3")==0) {// table
			Table(conn);
		}
		else if(strcmp(input, "4")==0) { //count
			Count(conn);
		}
		else if(strcmp(input, "5")==0) { // update small&big table
		    Update_SB(conn);
		}
		else if(strcmp(input, "6")==0) { // cls
		  clear();
		}
		else if(strcmp(input, "quit")==0) { // exit
      printf("\n===================================================================================================\n");
			printf("\nProgram QUIT\n");
      printf("\n===================================================================================================\n");
			return 0;
		}
		else {
      printf("\n===================================================================================================\n");
			printf("\nput the valid input\n");
      printf("\n===================================================================================================\n");
		}
	}
}

int file_size(FILE *fp) {
	int size;
	size = fseek(fp, 0, SEEK_END);
	return size;
}

void get_curtime() {
	time_t t = time(NULL);
	struct tm tm = *localtime(&t);

	sprintf(dateandtime, "%d-%d-%d %d:%d:%d", tm.tm_year+1900, tm.tm_mon+1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
}


bool Query(MYSQL * c, const char *q){
	if(mysql_query(c, q)){
    printf("\n===================================================================================================\n");
		fprintf(stderr, "###Warning###\n%d - %s", mysql_errno(conn), mysql_error(conn));
    printf("\n===================================================================================================\n");
		return FALSE;
	}
	return TRUE;
}

void Initialize(MYSQL *conn){

  nextUserID = 0;
  nextFileID = 0;
  nextSmallID = 0;
  nextBigID = 0;

  FILE *fp;
  char *line = NULL;
  size_t len = 0;
  ssize_t read;

  fp = fopen("/home/db_cw/EERD.sql", "r");
  if (fp == NULL) {
    printf("\n================================================================================================================\n");
    printf("initialization is failed.\nCheck .sql file.\n");
    printf("\n================================================================================================================\n");
    exit(EXIT_FAILURE);
  }
  while ((read = getline(&line, &len, fp)) != -1) {
    printf("%s",line);
    if(!(mysql_query(conn, line))&&(mysql_errno(conn)!=0)){
      printf("\n================================================================================================================\n");
  		fprintf(stderr, "###Warning###\n%d - %s", mysql_errno(conn), mysql_error(conn));
      printf("\n================================================================================================================\n");
  	}
    //mysql_free_result(res);
    //res=NULL;
  }
  fclose(fp);
  if (line)
      free(line);

  printf("Now all of the Tables are reset\n\n");
}

void Upload(MYSQL *conn){
	char path[255];
	FILE *fp;
	//Open File
	while(1){
		//Get Path
		printf("Put the file path:");
		scanf("%s",path);
		fp = fopen(path, "rb");
		if(fp==NULL){
			fprintf(stderr, "Wrong File. Try Again!\n");
		}else break;
	}
	//Get File Name
	char file_name[50];
	if(strrchr(path, '/')==NULL){	// If There is no '/' in path
		strcpy(file_name, path); // Path is Filename itself
	}else{	// If There is '/' in path
		strcpy(file_name, strrchr(path, '/')+1);	// Path is String after '/'
	}
	//Get File Size
	rewind(fp);
	fseek(fp, 0, SEEK_END);
	unsigned long file_size = ftell(fp);
	//Big or Small
	bool is_big;
	if(file_size>1024) is_big=TRUE;
	else is_big=FALSE;
	//Refer to Buf
	fseek(fp, 0, SEEK_SET);
	char * buf = (char *)malloc(file_size);
	fread(buf, sizeof(char), file_size, fp);
	fclose(fp);
	//Erase Escape Char
	char *buf_to = (char *)malloc(file_size*2+1);
	mysql_real_escape_string(conn, buf_to, buf, file_size);
	//Make Query (UPLOAD Metadata)
	char *temp_query_met = "INSERT INTO Metadata(UserID, FileName, FileSize) VALUES(%d, '%s', %d)";
	char *query_met = (char *)malloc(strlen(temp_query_met) + 100);
	sprintf(query_met, temp_query_met, user_id, file_name, file_size, dateandtime);
	//Query (UPLOAD Metadata)
	Query(conn, query_met);
	free(query_met);
	//Get FileID of Metadata
	char *temp_query_get = "SELECT FileID FROM Metadata WHERE FileName='%s' and UserID=%d";
	char *query_get = (char *)malloc(strlen(temp_query_get) + 20);
	sprintf(query_get, temp_query_get, file_name, user_id);
	unsigned long file_id;
	if(Query(conn, query_get)){
		res = mysql_use_result(conn);
		while((row=mysql_fetch_row(res))!=NULL){
			file_id = atoi(row[0]);
		}
	}
	free(query_get);
	//Make Query (UPLOAD SmallFile)
	char *temp_query_up_big = "INSERT INTO %s(DataPath, UpdatedCount, Hot, UserID, FileID) VALUES('%s', 0, 0, %d, %d)";
	char *temp_query_up_small = "INSERT INTO %s(Data, UpdatedCount, Hot, UserID, FileID) VALUES('%s', 0, 0, %d, %d)";
	char *query_up_big = (char *)malloc(strlen(temp_query_up_big)+(file_size*2)+50);
	char *query_up_small = (char *)malloc(strlen(temp_query_up_small)+(file_size*2)+50);
	//Query (UPLOAD SmallFile)
	if(is_big){
		sprintf(query_up_big, temp_query_up_big, "BigFile", path, user_id, file_id);
		Query(conn, query_up_big);
	}else{
		sprintf(query_up_small, temp_query_up_small, "SmallFile", buf_to, user_id, file_id);
		Query(conn, query_up_small);
	}
	free(query_up_big);
	free(query_up_small);
	free(buf);
	free(buf_to);
	clear();
}

void UserChange(MYSQL *conn){
	clear();
	while(1){
		//Print Out User List
		ListUser(conn);
		//Get User Name Input
		char temp_name[50];
		printf("\nPut the User Name (To Quit, type \'quit\'): ");
		scanf("%s", temp_name);
		if(strcmp(temp_name, "quit")==0){
			break;
		}
		char *temp_query = "SELECT UserID from User WHERE UserName = '%s';";
		char *query = (char *) malloc(strlen(temp_query) + 20);
		sprintf(query, temp_query, temp_name);
		if(Query(conn, query)){
			res = mysql_use_result(conn);
			if((row=mysql_fetch_row(res))!=NULL){
				strcpy(user_name, temp_name);
				free(query);
				while(row!=NULL){	// To avoid ERROR: 'Commands out of sync; you can't run this command now'
					user_id = atoi(row[0]);
		      printf("\n================================================================================================================\n");
				  printf("User selected!\n User ID : %d\tUser Name : %s", user_id, user_name);
		      printf("\n================================================================================================================\n");
					row=mysql_fetch_row(res);
				}
				select_first=FALSE;
				break;
			}
		}
	  printf("\n================================================================================================================\n");
		printf("No User whose name is \'%s\'. Please Type Again. ", temp_name);
	  printf("\n================================================================================================================\n");
		free(query);
	}

}

void Table(MYSQL *conn){
  printf("%s 's Data\n", user_name);

  char *temp_query1 = "select Metadata.FileID, Metadata.FileName, Metadata.FileSize, Metadata.ModifyTime, UpdatedCount from SmallFile,Metadata where Metadata.FileID = SmallFile.FileID AND  Metadata.UserID=%d;";
	char *query1 = (char *) malloc(strlen(temp_query1) + 20);
	sprintf(query1, temp_query1, user_id);
	Query(conn, query1);
  res = mysql_use_result(conn);
  if(res!=0){
    printf("SmallFile List\n");
	  printf("================================================================================================================\n");
    printf("%20s | %20s | %20s | %20s | %20s\n", "FileID", "FileName","FileSize", "ModifyTime", "UpdatedCount");
    printf("================================================================================================================\n");
    while((row = mysql_fetch_row(res))!=NULL){
      printf("%20s | %20s | %20s | %20s | %20s\n", row[0], row[1], row[2], row[3], row[4]);
    }
    printf("================================================================================================================\n");
  }

  char *temp_query2 = "select Metadata.FileID, Metadata.FileName, Metadata.FileSize, Metadata.ModifyTime, UpdatedCount from BigFile,Metadata where Metadata.FileID = BigFile.FileID AND Metadata.UserID=%d;";
	char *query2 = (char *) malloc(strlen(temp_query2) + 20);
	sprintf(query2, temp_query2, user_id);
	Query(conn, query2);
  res = mysql_use_result(conn);
  if(res!=0){
    printf("BigFile List\n");
	  printf("================================================================================================================\n");
    printf("%20s | %20s | %20s | %20s | %20s\n", "FileID", "FileName","FileSize", "ModifyTime", "UpdatedCount");
    printf("================================================================================================================\n");
    while((row = mysql_fetch_row(res))!=NULL){
      printf("%20s | %20s | %20s | %20s | %20s\n", row[0], row[1], row[2], row[3], row[4]);
    }
    printf("================================================================================================================\n");
  }

  free(query1);
  free(query2);
}

void Count(MYSQL *conn){
  printf("%s 's the number of files\n", user_name);
  printf("             |      HOT      |      Cold      |\r\n");
  PrintCount(conn, "SmallFile");
  PrintCount(conn, "BigFile");
}

void PrintCount(MYSQL *conn, char *table){
  char *temp_query = "SELECT ifnull(sum(case when HOT=1 then 1 else 0 end), 0) as HOT, ifnull(sum(case when HOT=0 then 1 else 0 end), 0) as COLD FROM SDS_group4.%s where UserID=%d";
  char *query = (char *) malloc(strlen(temp_query) + 20);
  sprintf(query, temp_query, table, user_id);
  Query(conn, query);
  res = mysql_use_result(conn);
  if(res != 0){
    while((row = mysql_fetch_row(res))!=NULL){
      printf(" %10s  |   %10s  |    %10s  |\r\n", table, row[0], row[1]);
    }
  }
  free(query);
}

void Update_SB(MYSQL *conn){
  srand(time(NULL));
  int sb = rand()%2; // select randomly small(0) or big(1)
  int f_id;
  char *temp_query1 = "select Metadata.FileID from %s,Metadata where Metadata.FileID = %s.FileID AND Metadata.UserID=%d order by rand() limit 1;";
  char *temp_query2 = "update %s set UpdatedCount=UpdatedCount+1 where FileID=%d;";
  char *temp_query3 = "update %s set HOT = 1 WHERE FileID=%d and UpdatedCount >= 5;";
  char *query1 = (char *) malloc(strlen(temp_query1)+30);
  char *query2 = (char *) malloc(strlen(temp_query2)+30);
  char *query3 = (char *) malloc(strlen(temp_query3)+30);

  if(sb == 0) {
    sprintf(query1, temp_query1, "SmallFile","SmallFile", user_id);
    Query(conn, query1);
    res = mysql_use_result(conn);
    if(res!=0){
      f_id = -1;
      while((row = mysql_fetch_row(res))!=NULL){
        f_id = atoi(row[0]);
      }
    }

    printf("%d", f_id);

    if(f_id != -1){
      sprintf(query2, temp_query2, "SmallFile", f_id);
      if(Query(conn, query2)){
        res = mysql_use_result(conn);
        sprintf(query3, temp_query3, "SmallFile", f_id);
        if(Query(conn, query3)) {
        res = mysql_use_result(conn);
        printf("\n================================================================================================================\n");
        printf("Updating SmallFile is Successed!\n");
        printf("================================================================================================================\n");
        }
      }
    }
    else {
      printf("\n================================================================================================================\n");
      printf("Tried updating randomly in SmallFile, But there is no files to update.\n");
      printf("================================================================================================================\n");
    }
  }
  else {
    sprintf(query1, temp_query1, "BigFile", "BigFile", user_id);
    Query(conn, query1);
    res = mysql_use_result(conn);
    if(res!=0){
      f_id = -1;
      while((row = mysql_fetch_row(res))!=NULL){
        f_id = atoi(row[0]);
      }
    }

    printf("%d", f_id);

    if(f_id != -1){
      sprintf(query2, temp_query2, "BigFile", f_id);
      if(Query(conn, query2)){
        res = mysql_use_result(conn);
        sprintf(query3, temp_query3, "BigFile", f_id);
        if(Query(conn, query3)){
          res = mysql_use_result(conn);
          printf("\n================================================================================================================\n");
          printf("Updating BigFile is Successed!\n");
          printf("================================================================================================================\n");
          res = mysql_use_result(conn);
        }
      }
    }
    else {
      printf("\n================================================================================================================\n");
      printf("Tried updating randomly in BigFile, But there is no files to update.\n");
      printf("================================================================================================================\n");
    }
  }
  free(query1);
  free(query2);
  free(query3);
}

void AddUser(MYSQL *conn){
	int usr_id;
	char usr_name[20];
	printf("\nType New User's Name: ");
	scanf("%s", usr_name);

	char *temp_query = "INSERT INTO User(UserName) VALUES('%s')";
	char *query = (char *)malloc(strlen(temp_query) + 20);
	sprintf(query, temp_query, usr_name);
	Query(conn, query);
	free(query);
}

void ListUser(MYSQL *conn){
	Query(conn, "SELECT * FROM User");
	  res = mysql_use_result(conn);
	  if(res!=0){
	    printf("User List\n");
		  printf("================================================================================================================\n");
	    printf("%20s | %20s \n", "UserID", "UserName");
	    printf("================================================================================================================\n");
	    while((row = mysql_fetch_row(res))!=NULL){
	      printf("%20s | %20s \n", row[0], row[1]);
	    }
	    printf("================================================================================================================\n");
	  }
}
