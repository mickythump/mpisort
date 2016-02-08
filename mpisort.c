#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

int *merge(int *arr1, int arr1_size, int *arr2, int arr2_size)
{
  int i = 0, j = 0, k = 0;
  int *result_array;

  result_array = (int *)malloc((arr1_size + arr2_size) * sizeof(int)); //result array's size is sum of 1st and 2nd sizes
  while(i < arr1_size && j < arr2_size) { //while there are elements in both 1st and 2nd array
    if(arr1[i] < arr2[j]) { //if 1st array element is lower, put it to result, increment 1st index
      result_array[k] = arr1[i];
      i++;
    }
    else { //if 2nd array element is lower, put it to result, increment 2nd index
      result_array[k] = arr2[j];
      j++;
    }
    k++; //increment result index
  }
  if(i == arr1_size) {//if 1st array is empty
    while(j < arr2_size) { //put the rest of the 2nd array to result
      result_array[k] = arr2[j];
      j++;
      k++;
    }
  }
  else { //if 2nd array is empty
    while(i < arr1_size) { //put the rest of the 1st array to result
      result_array[k] = arr1[i];
      i++;
      k++;
    }
  }
  return result_array;
}

int my_compare (const void * a, const void * b) //helper function to qsort
{
    int _a = *(int*)a;
    int _b = *(int*)b;
    if(_a < _b) return -1;
    else if(_a == _b) return 0;
    else return 1;
}

int main(int argc, char  *argv[]) {
  int numtasks, rank; //number of processes, id of process
  int piece_size, *piece, *piece2, piece2_size; //chunk arrays and their sizes

  int i=0;
  double arr_size; //array size
  int *arr;
  int step = 0; // merge step number
  MPI_Status status;
  clock_t begin, end, begin_sort, end_sort;
  double time_spent, time_sort;

  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &numtasks);

  if(rank == 0) {
    printf("Enter number of data to sort: ");
    scanf("%lf", &arr_size);

    srand(time(NULL));

    arr = (int *)malloc(arr_size * sizeof(int));

    for(i = 0; i < arr_size; i++) { //generate random data and put them into array
      *(arr + i) = rand() % (int)arr_size;
    }

    if((int)arr_size % numtasks != 0) {
      printf("The number of data to sort is not divisible by the number of processes.");
      MPI_Finalize();
      exit(1);
    } else {
      piece_size = (int)(arr_size / numtasks); //calculate chunk array size
    }
  }

  begin = clock();

  MPI_Bcast(&piece_size, 1, MPI_INT, 0, MPI_COMM_WORLD); //broadcast chunk size to processes

  piece = (int *)malloc(piece_size * sizeof(int));

  MPI_Scatter(arr, piece_size, MPI_INT, piece, piece_size, MPI_INT, 0, MPI_COMM_WORLD); //send array items to processes

  begin_sort = clock();

  qsort(piece, piece_size, sizeof(int), my_compare); //sort arrays in processes

  end_sort = clock();
  time_sort += (double)(end_sort - begin_sort) / CLOCKS_PER_SEC;

  //Merging qsorted arrays
  step = 1;
  while(step < numtasks)
  {
    if(rank % (2 * step) == 0)
    {
      if(rank + step < numtasks)
      {
        MPI_Recv(&piece2_size, 1, MPI_INT, rank + step, 0, MPI_COMM_WORLD, &status); //receive array size of neighbour process
        piece2 = (int *)malloc(piece2_size * sizeof(int));
        MPI_Recv(piece2, piece2_size, MPI_INT, rank + step, 0, MPI_COMM_WORLD, &status); //receive neighbour process' array
        begin_sort = clock();
        piece = merge(piece, piece_size, piece2, piece2_size); //merge n process array with n+step process array
        end_sort = clock();
        time_sort += (double)(end_sort - begin_sort) / CLOCKS_PER_SEC;
        piece_size = piece_size + piece2_size; //increase array size
      }
    }
    else
    {
      int nearest_process = rank - step; //calculate nearest process to send an array
      MPI_Send(&piece_size, 1, MPI_INT, nearest_process, 0, MPI_COMM_WORLD); //send array size to the nearest process
      MPI_Send(piece, piece_size, MPI_INT, nearest_process, 0, MPI_COMM_WORLD); //sesand array to the nearest process
      break;
    }
    step = step * 2;
  }

  if(rank == 0)
  {
    end = clock();
    time_spent = (double)(end - begin) / CLOCKS_PER_SEC;

    printf("Time spent parallel: %f\n", time_spent);
    printf("Time spent sorting parallel: %f\n", time_sort);

    begin = clock();
    qsort(arr, arr_size, sizeof(int), my_compare); //sort the same array sequentially
    end = clock();
    time_spent = (double)(end - begin) / CLOCKS_PER_SEC;

    printf("Time spent seq: %f\n", time_spent);

    FILE *output = fopen("sorted.data", "w");
    for(i = 0; i < piece_size; i++)
    {
      fprintf(output, "%d\n", piece[i]); //save data to file
    }
    fclose(output);
  }

  MPI_Finalize();
  return 0;
}
