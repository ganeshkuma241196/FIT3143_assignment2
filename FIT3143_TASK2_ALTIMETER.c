#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <mpi.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>

#define MSG_EXIT 1
#define MSG_PRINT_UNORDERED 2
#define SIZE 10

int master_io(MPI_Comm world_comm, MPI_Comm comm);
int slave_io(MPI_Comm world_comm, MPI_Comm comm);
void* altimeter(void *pArg);
void insert(float* array);
void delete();
int back=-1;
int front=-1;
int threshhold;
int width;
int length;

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
     

	while (1) {
		pthread_mutex_lock(&g_Mutex);
        float randomVal = ((float)rand_r(&seed)/(float)(RAND_MAX)) * 1500 + threshhold;

        int randomlength=  rand_r(&seed) % length ; //x coord
        
        int randomwidth= rand_r(&seed) % width; //y coord

        float randArray[3]={randomVal,(float)randomlength,(float)randomwidth} ;//reading,X coord, Y coord

        insert(randArray);



		
		pthread_mutex_unlock(&g_Mutex);

        sleep(2000);
	}
	printf("Thread finished\n");
	fflush(stdout);

	return 0;
}

/* This is the master */
int master_io(MPI_Comm world_comm, MPI_Comm comm)
{
	int size;
	MPI_Comm_size(world_comm, &size );
	g_nslaves = size - 1;
	
	pthread_t tid;
	pthread_mutex_init(&g_Mutex, NULL);
	pthread_create(&tid, 0, altimeter, NULL); // Create the thread

	char buf[256];
	MPI_Status status;
	while (1) {
		pthread_mutex_lock(&g_Mutex);
		if(g_nslaves <= 0){
			pthread_mutex_unlock(&g_Mutex);
			break;
		}
		MPI_Recv(buf, 256, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status );
		switch (status.MPI_TAG) {
			case MSG_EXIT: 
			{
				g_nslaves--; 
				printf("MPI Master Process. g_nslaves: %d\n", g_nslaves);
				break;
			}
			case MSG_PRINT_UNORDERED:
			{
				printf("MPI Master Process prints: %s", buf);
				fflush(stdout);
				break;
			}
			default:
			{
				break;
			}
		}
		pthread_mutex_unlock(&g_Mutex);
	}
	printf("MPI Master Process finished\n");
	fflush(stdout);
	
	pthread_join(tid, NULL);
	pthread_mutex_destroy(&g_Mutex);
	
    return 0;
}

/* This is the slave */
int slave_io(MPI_Comm world_comm, MPI_Comm comm)
{
	int ndims=2, size, my_rank, reorder, my_cart_rank, ierr, masterSize;
	MPI_Comm comm2D;
	int dims[ndims],coord[ndims];
	int wrap_around[ndims];
	char buf[256];
    
    	MPI_Comm_size(world_comm, &masterSize); // size of the master communicator
  	MPI_Comm_size(comm, &size); // size of the slave communicator
	MPI_Comm_rank(comm, &my_rank);  // rank of the slave communicator
	dims[0]=dims[1]=0;
	
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


