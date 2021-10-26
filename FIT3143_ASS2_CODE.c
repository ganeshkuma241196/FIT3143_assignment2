#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <mpi.h>
#include <time.h>
#include <math.h>
#include <unistd.h>
#include <pthread.h>

#define MSG_EXIT 1
#define MSG_PRINT_UNORDERED 2
#define SIZE 10
#define SHIFT_ROW 0
#define SHIFT_COL 1
#define DISP 1

int master_io(MPI_Comm world_comm, MPI_Comm comm);
int slave_io(MPI_Comm world_comm, MPI_Comm comm);
void* altimeter(void *pArg);
void insert(float* array);
void delete();
int back=-1;
int front=-1;
int threshhold;
int iterations;
int width;
int length;
int recievedInput=false;
float movingAverage;
bool terminate=false;
int matches=0;
int totalMessages=0;
int totalaltimeterreadings=0;





pthread_mutex_t g_Mutex = PTHREAD_MUTEX_INITIALIZER;
int g_nslaves = 0;
float* altimeterReadings[SIZE];


int main(int argc, char **argv)
{

    int rank, size;
    MPI_Comm new_comm;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);


	if(argc == 5) {
		length= atoi (argv[1]);
		width= atoi (argv[2]);
		threshhold=atoi (argv[3]);
		iterations=atoi (argv[4]);
		recievedInput=true;
		
		if((length*width) != size-1) {
			if(rank == 0) printf("Error length*width = %d*%d = %d != %d\n", length, width, length*width, size-1);
			MPI_Finalize();
			
			return 0;
		}
	}

	else {
			length = width = (int)sqrt(size);
			threshhold=6000;
			iterations=100;
			
	}

    
    MPI_Comm_split( MPI_COMM_WORLD,rank == size-1, 0, &new_comm);
    if (rank == size-1) 
	master_io( MPI_COMM_WORLD, new_comm );
    else
	slave_io( MPI_COMM_WORLD, new_comm );
    MPI_Finalize();
    return 0;
}

void* altimeter(void *pArg) // Common function prototype
{
	unsigned int seed = time(NULL);
     

	while (!terminate) {
		pthread_mutex_lock(&g_Mutex);
        float randomVal = ((float)rand_r(&seed)/(float)(RAND_MAX)) * 1500 + threshhold;

        int randomlength=  rand_r(&seed) % length ; //x coord
        
        int randomwidth= rand_r(&seed) % width; //y coord

		unsigned int timenow= time(NULL);

        float randArray[4]={randomVal,(float)timenow,(float)randomlength,(float)randomwidth} ;//reading,X coord, Y coord

        insert(randArray);
		totalaltimeterreadings+=1;



		
		pthread_mutex_unlock(&g_Mutex);

        sleep(1000);
	}
	printf("Thread finished\n");
	fflush(stdout);

	return 0;
}

void* sensor_comm(void *pArg)
{
	int size;
	MPI_Status status;
	MPI_Comm_size(MPI_COMM_WORLD, &size );
	MPI_Comm* p = (MPI_Comm*)pArg;
	MPI_Comm comm2D= *p;
	

	while(!terminate)
	{
		MPI_Request sensor_request;
	    MPI_Status sensor_status;
		MPI_Request base_request;
	    MPI_Status base_status;
		int sensorFlag;
	    int baseFlag;
		float useless;
		
		

		MPI_Irecv(&useless, 1, MPI_FLOAT, MPI_ANY_SOURCE, 0, comm2D, &sensor_request);
		MPI_Irecv(&useless, 1, MPI_FLOAT, size-1, 66, MPI_COMM_WORLD, &base_request);

		MPI_Test(&sensor_request,&sensorFlag,&sensor_status);
		MPI_Test(&base_request,&baseFlag,&base_status);

		if (baseFlag!=0)
		{
			terminate=true;
			break;
		}

		if (sensorFlag!=0)
		{
			pthread_mutex_lock(&g_Mutex);
			int sensor=sensor_status.MPI_SOURCE;
			MPI_Isend(&movingAverage, 1, MPI_FLOAT, sensor , 1, comm2D, &sensor_request);
			MPI_Test(&sensor_request,&sensorFlag,&sensor_status);
			pthread_mutex_unlock(&g_Mutex);
			printf("We are here%d\n",sensorFlag);
	        fflush(stdout);

		}
		


	}

	return 0;
}

void* base_comm(void *pArg)
{
	FILE *fp;



    
	
	while(!terminate)
	{
	


	}

	
	return 0;
	
	
}

/* This is the master */
int master_io(MPI_Comm world_comm, MPI_Comm comm)
{
	unsigned int starttime= time(NULL);
	int size;
	MPI_Comm_size(world_comm, &size );
	g_nslaves = size - 1;
    FILE *fp;
	
	pthread_t tid;
	//pthread_t tid2;

	pthread_mutex_init(&g_Mutex, NULL);
	pthread_create(&tid, 0, altimeter, NULL); // Create the thread
	//pthread_create(&tid2, 0, base_comm, NULL); // Create the thread


	char buf[256];
	MPI_Status status;
	for(int i=0;i<iterations;i++)  {

	sleep(1000);

	MPI_Request base_request;
	MPI_Status base_status;
	int sendFlag;
	float sendArray[5];
	MPI_Irecv( sendArray, 5, MPI_FLOAT, MPI_ANY_SOURCE, 2, MPI_COMM_WORLD ,&base_request);
	MPI_Test(&base_request,&sendFlag,&base_status);

	if(sendFlag!=0)
	{
		printf("we have started");

		pthread_mutex_lock(&g_Mutex);
		totalMessages+=1;
		int loop;
		bool found=false;
		if(front>back)
		{

			loop=SIZE;

		}
		
		else if(front==-1)
		{
			loop=0;

		}

		else{
			loop=back-front+1;
		}

		

		for(int i=0;i<loop;i++)
		{
			if(altimeterReadings[i][2]==sendArray[2])
			{
				if(altimeterReadings[i][3]==sendArray[3])
				{
					if(abs(altimeterReadings[i][1]-sendArray[1])<=100)
					{
						if(abs(altimeterReadings[i][0]-sendArray[0])<=300)
					    {
						int theTime=(int)sendArray[1];
						int theTime2=(int)altimeterReadings[i][1];
						matches+=1;
						time_t  now= (time_t)theTime;
						time_t  now2= (time_t)theTime2;
                        struct tm ts;
						
                        char buf2[80];
						char buf3[80];
						ts = *localtime(&now);
                        strftime(buf2, sizeof(buf2), "%a %Y-%m-%d %H:%M:%S", &ts);
						ts = *localtime(&now2);
						strftime(buf3, sizeof(buf3), "%a %Y-%m-%d %H:%M:%S", &ts);
						found=true;
						fp = fopen("outputBase2.txt","w");
						fprintf(fp, "True alert:\n");
						fprintf(fp, "\t Coordinates:%d,%d\n", (int)sendArray[2],(int)sendArray[3]);
						fprintf(fp,"\t Sensor datetime:%s\n",buf2);
						fprintf(fp,"\t Altimeter datetime:%s\n",buf3);
						fprintf(fp,"\t Sensor column height:%f\n",sendArray[0]);
						fprintf(fp,"\t altimeter column height:%f\n",altimeterReadings[i][0]);
						fprintf(fp,"\t No. of adjacent sensors exceeding with values similar:%d\n",(int)sendArray[4]);
						fclose(fp);
						
						break;

						}
						


					}
				}
			}
			
		}
		pthread_mutex_unlock(&g_Mutex);
		

		if (!found)
		{
			int theTime=(int)sendArray[1];
					
			time_t  now= (time_t)theTime;
						
            struct tm ts;
						
            char buf2[80];
						
			ts = *localtime(&now);
            strftime(buf2, sizeof(buf2), "%a %Y-%m-%d %H:%M:%S", &ts);
			fp = fopen("outputBase2.txt","w");
						
			fprintf(fp, "False alert:\n");
			fprintf(fp, "\t Coordinates:%d,%d\n", (int)sendArray[2],(int)sendArray[3]);
			fprintf(fp,"\t Sensor datetime:%s\n",buf2);
			fprintf(fp,"\t Sensor column height:%f\n",sendArray[0]);
			fprintf(fp,"\t No. of adjacent sensors exceeding with values similar:%d\n",(int)sendArray[4]);
			fclose(fp);


		}


	}

		
	}
	terminate=true;
	float useless=501/212;
	MPI_Request send_request[size-1];
	MPI_Status send_status[size-1];

	for(int i=0;i<size-1;i++)
	{
		MPI_Isend(&useless, 1, MPI_FLOAT, i, 66, world_comm, &send_request[i]);
	}

	MPI_Waitall(size-1, send_request, send_status);

    printf("we have executed order 66");

	unsigned int endtime= time(NULL);

	int simTime=endtime-starttime;

	fp = fopen("outputBase.txt","w");

	fprintf(fp, "Simulation summary:\n");
	fprintf(fp, "\t Sim time:%d seconds\n", simTime);
	fprintf(fp,"\t Total alerts recieved:%d\n",totalMessages);
	fprintf(fp,"\t Total true alerts:%d\n",matches);
	fprintf(fp,"\t No. of altimeter readings:%d\n",totalaltimeterreadings);

	fclose(fp);





	printf("MPI Master Process finished\n");
	fflush(stdout);

	
	pthread_join(tid, NULL);
	//pthread_join(tid2, NULL);
	pthread_mutex_destroy(&g_Mutex);
	
    return 0;
}

/* This is the slave */
int slave_io(MPI_Comm world_comm, MPI_Comm comm)
{
	int ndims=2, size, my_rank, reorder, my_cart_rank, ierr, masterSize;
	int nbr_i_lo, nbr_i_hi;
	int nbr_j_lo, nbr_j_hi;
	MPI_Comm comm2D;
	int dims[ndims],coord[ndims];
	int wrap_around[ndims];
	char buf[256];
    
    MPI_Comm_size(world_comm, &masterSize); // size of the master communicator
  	MPI_Comm_size(comm, &size); // size of the slave communicator
	MPI_Comm_rank(comm, &my_rank);  // rank of the slave communicator

	if(recievedInput)
	{
		dims[0]= length;
		dims[1]=width;
	}
	else{
		dims[0]=dims[1]=0;

	}
	
	
	MPI_Dims_create(size, ndims, dims);
    	if(my_rank==0)
		printf("Slave Rank: %d. Comm Size: %d: Grid Dimension = [%d x %d] \n",my_rank,size,dims[0],dims[1]);

    	/* create cartesian mapping */
	wrap_around[0] = 0;
	wrap_around[1] = 0; /* periodic shift is .false. */
	reorder = 0;
	ierr =0;
	ierr = MPI_Cart_create(comm, ndims, dims, wrap_around, reorder, &comm2D);
	if(ierr != 0) printf("ERROR[%d] creating CART\n",ierr);
	
	/* find my coordinates in the cartesian communicator group */
	MPI_Cart_coords(comm2D, my_rank, ndims, coord); // coordinated is returned into the coord array
	/* use my cartesian coordinates to find my rank in cartesian group*/
	MPI_Cart_rank(comm2D, coord, &my_cart_rank);

	MPI_Cart_shift( comm2D, SHIFT_ROW, DISP, &nbr_i_lo, &nbr_i_hi );
	MPI_Cart_shift( comm2D, SHIFT_COL, DISP, &nbr_j_lo, &nbr_j_hi );

	

	unsigned int seed = time(NULL);
	float val1= ((float)rand_r(&seed)/(float)(RAND_MAX)) * 2500 + threshhold-1000;
	float val2= ((float)rand_r(&seed)/(float)(RAND_MAX)) * 2500 + threshhold-1000;
	float val3= ((float)rand_r(&seed)/(float)(RAND_MAX)) * 2500 + threshhold-1000;
	float val4= ((float)rand_r(&seed)/(float)(RAND_MAX)) * 2500 + threshhold-1000;
	movingAverage= (val1+val2+val3+val4)/4;

	pthread_t tid;
	pthread_mutex_init(&g_Mutex, NULL);
	pthread_create(&tid, 0, sensor_comm, &comm2D); // Create the thread

	while(!terminate)
	{
		pthread_mutex_lock(&g_Mutex);
		float val1= val2;
	    float val2= val3;
	    float val3= val4;
	    float val4= ((float)rand_r(&seed)/(float)(RAND_MAX)) * 2500 + threshhold-1000;
	    movingAverage= (val1+val2+val3+val4)/4;
        printf("Slave Rank: %d. Moving average%f ] \n",my_rank,movingAverage);
		pthread_mutex_unlock(&g_Mutex);

		if (movingAverage>threshhold)
		{
			

			MPI_Request send_request[4];
	        MPI_Request receive_request[4];
	        MPI_Status send_status[4];
	        MPI_Status receive_status[4];
			int sendFlag;
			int receiveFlag;

			float sma=((float)rand_r(&seed)/(float)(RAND_MAX));

			MPI_Isend(&sma, 1, MPI_FLOAT, nbr_i_lo, 0, comm2D, &send_request[0]);
			MPI_Isend(&sma, 1, MPI_FLOAT, nbr_i_hi, 0, comm2D, &send_request[1]);
			MPI_Isend(&sma, 1, MPI_FLOAT, nbr_j_lo, 0, comm2D, &send_request[2]);
			MPI_Isend(&sma, 1, MPI_FLOAT, nbr_j_hi, 0, comm2D, &send_request[3]);

			float recvValL=-100/2 , recvValR=-100/2 , recvValT=-100/2 , recvValB=-100/2 ;
			MPI_Irecv(&recvValT, 1, MPI_FLOAT, nbr_i_lo, 1, comm2D, &receive_request[0]);
			MPI_Irecv(&recvValB, 1, MPI_FLOAT, nbr_i_hi, 1, comm2D, &receive_request[1]);
			MPI_Irecv(&recvValL, 1, MPI_FLOAT, nbr_j_lo, 1, comm2D, &receive_request[2]);
			MPI_Irecv(&recvValR, 1, MPI_FLOAT, nbr_j_hi, 1, comm2D, &receive_request[3]);

			MPI_Waitall(4, send_request,  send_status);
			MPI_Waitall(4, receive_request, receive_status);

			float MArray[4]= {recvValL,recvValR,recvValT,recvValB};
			int match = 0;

			for (int i=0;i<4;i++)
			{
				if(MArray[i]>0)
				{
					if(abs(movingAverage - MArray[i]) <= 100)
				{
				    match += 1;
				}

				}
				
				
				


			}

			if(match>=2)
			{
			    unsigned int timestamp= time(NULL);
				float sendArray[5]={movingAverage,(float)timestamp,(float)coord[0],(float)coord[1],(float)match};
				MPI_Request base_request;
	            MPI_Status base_status;
				MPI_Isend( sendArray, 5, MPI_FLOAT, masterSize-1, 2, world_comm ,&base_request);
				MPI_Test(&base_request,&sendFlag,&base_status);
                printf("Slave Rank: %d. base send%f ] \n",my_rank,movingAverage);

			}
			
		}


		

		


		
	}

	MPI_Comm_free(&comm2D);

	pthread_join(tid,NULL);
	pthread_mutex_destroy(&g_Mutex);

	return 0;






}

void insert(float* array)
{
    if(back>=SIZE-1)
    {
        if(front>0)
        {
            back=0;
            altimeterReadings[back]=array;
        }
        else
        {
            delete();
            back=0;
            altimeterReadings[back]=array;


        }
    }

    else if (back+1==front)
    {
       delete();
       back=back+1;
       altimeterReadings[back]=array;
    }

    else if(back==-1)
    {
        front=0;
        back=0;
        altimeterReadings[back]=array;
    }

    else{
        back=back+1;
        altimeterReadings[back]=array;
    }
}

void delete()
{
    if(front>= SIZE-1)
    {
        front=0;
    }
    else{
        front=front+1;
    }
}


