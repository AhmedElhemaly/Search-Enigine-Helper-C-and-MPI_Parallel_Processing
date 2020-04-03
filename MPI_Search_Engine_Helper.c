#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mpi.h"
char indexes [50][3] = {"1","2","3","4","5","6","7","8","9","10",
                        "11","12","13","14","15","16","17","18","19","20",
                        "21","22","23","24","25","26","27","28","29","30",
                        "31","32","33","34","35","36","37","38","39","40",
                        "41","42","43","44","45","46","47","48","49","50"
                       };
struct String
{
    char arr[300];
};
char query[300];
int main(int argc, char * argv[])
{
    int rank, size, i,ps,rem;
    int c=0;
    int totalSum =0;
    MPI_Init( &argc, &argv );
    MPI_Comm_rank( MPI_COMM_WORLD, &rank );
    MPI_Comm_size( MPI_COMM_WORLD, &size );
    MPI_Status status;
    double start_time, end_time;
    struct String str_send[1500];
    int j=0;
    if(rank == 0)
    {
        start_time = MPI_Wtime();

        printf("Enter your query : ");
        scanf("%[^\n]",query);

        for(i=1; i<size; i++)
        {
            MPI_Send(query,300,MPI_CHAR, i, 10, MPI_COMM_WORLD);
        }
        //printf(query);
        ps = 50/(size-2);
        rem = 50%ps;
        printf("PS = %d , rem = %d\n",ps,rem);
        MPI_Send(&rem,1,MPI_INT, 1, 0, MPI_COMM_WORLD);

        for (i = 2 ; i < size; i++ )
        {
            MPI_Send(&ps,1,MPI_INT, i, 0, MPI_COMM_WORLD);
            MPI_Send(&rem,1,MPI_INT, i, 0, MPI_COMM_WORLD);
        }
        char line[300];
        int counter11;
        MPI_Recv(&counter11,1,MPI_INT, 1, 3, MPI_COMM_WORLD, &status);
        totalSum+=counter11;
        for(i=0; i<counter11; i++)
        {
            MPI_Recv(line,300,MPI_CHAR, 1, 4, MPI_COMM_WORLD, &status);
            FILE * fp2;
            fp2 = fopen ("out.txt","a");
            fprintf (fp2, line);
            fprintf (fp2, "\n");
            fclose (fp2);
        }
        for(i=2; i<size; i++)
        {
            MPI_Recv(&counter11,1,MPI_INT, i, 5, MPI_COMM_WORLD, &status);
            totalSum+=counter11;
            for(j=0; j<counter11; j++)
            {
                MPI_Recv(line,300,MPI_CHAR, i, 6, MPI_COMM_WORLD, &status);

                FILE * fp2;
                fp2 = fopen ("out.txt","a");
                fprintf (fp2, line);
                fprintf (fp2, "\n");
                fclose (fp2);
            }
        }
        printf("Total Sum : %d\n",totalSum);

        FILE *fp;
        fp = fopen("out.txt","a");
        fputs("Query : ",fp);
        fputs(query,fp);
        fputs("\n",fp);
        fputs("Search Results Found = ",fp);
        fprintf(fp, "%d", totalSum);
        fputs("\n",fp);
        fclose(fp);

        end_time = MPI_Wtime();
        printf("Execution time on %2d nodes: %f\n", size, end_time-start_time);
    }
    else if(rank==1)
    {
        MPI_Recv(&rem, 1,MPI_INT, 0, 0, MPI_COMM_WORLD, &status);
        MPI_Recv(query, 300,MPI_CHAR, 0, 10, MPI_COMM_WORLD, &status);
        char qy[300];
        strcpy(qy,query);
        puts(qy);
        FILE *fp;
        for(i=0; i<rem; i++)
        {
            char fName[]= "Aristo-Mini-Corpus/Aristo-Mini-Corpus P-";
            strcat(fName,indexes[i]);
            strcat(fName,".txt");
            fp = fopen(fName, "r");
            if(fp == NULL)
            {
                perror("Error opening file");
                return(-1);
            }
            while( fgets (str_send[j].arr, 300, fp)!=NULL )
            {
                strcpy(qy,query);
                char * token = strtok(qy, " ");
                int ok = 1;
                while(token!=NULL)
                {
                    if(!strstr(str_send[j].arr,token))
                    {

                        ok = 0;
                        break;
                    }
                    token = strtok(NULL, " ");
                }
                if(ok)
                {
                    MPI_Send(str_send[j].arr,300,MPI_CHAR, 0, 4, MPI_COMM_WORLD);
                    c++;
                }
                j++;
            }
        }
        printf("C form rank 1 = %d \n",c);
        int counter1 = c;
        MPI_Send(&counter1,1,MPI_INT, 0, 3, MPI_COMM_WORLD);

    }
    else
    {
        MPI_Recv(query, 300,MPI_CHAR, 0, 10, MPI_COMM_WORLD, &status);
        char qy[300];
        strcpy(qy,query);
        MPI_Recv(&ps, 1,MPI_INT, 0, 0, MPI_COMM_WORLD, &status);
        MPI_Recv(&rem, 1,MPI_INT, 0, 0, MPI_COMM_WORLD, &status);

        FILE *fp;
        int beg= (rank-2)*ps+rem;
        for(i=beg; i<beg+ps; i++)
        {
            char fName[]= "Aristo-Mini-Corpus/Aristo-Mini-Corpus P-";
            strcat(fName,indexes[i]);

            strcat(fName,".txt");
            fp = fopen(fName, "r");
            if(fp == NULL)
            {
                perror("Error opening file");
                return(-1);
            }
            while( fgets (str_send[j].arr, 300, fp)!=NULL )
            {
                strcpy(qy,query);
                char * token = strtok(qy, " ");
                int ok = 1;
                while(token!=NULL)
                {
                    if(!strstr(str_send[j].arr,token))
                    {
                        ok = 0;
                        break;
                    }
                    token = strtok(NULL, " ");
                }
                if(ok)
                {
                    MPI_Send(str_send[j].arr,300,MPI_CHAR, 0, 6, MPI_COMM_WORLD);
                    c++;
                }
                j++;
            }
        }
        printf("C = %d \n",c);
        MPI_Send(&c,1,MPI_INT, 0, 5, MPI_COMM_WORLD);
    }
    MPI_Finalize();
}
