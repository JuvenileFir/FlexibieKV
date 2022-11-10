// time count part
// semaphore
#include <mutex>
#include <sys/time.h>
#include <iostream>

using namespace std;

class Timer{
  public:

  double time[30];
  mutex timeMutex[30];
  double timebase;

  int startCount[30];
  int endCount[30];

  Timer() {
    for (int i = 0; i < 30; i++) {
      time[i] = 0.0;
    }
    struct timeval t1;                           
    gettimeofday(&t1, NULL);
    timebase = t1.tv_sec * 1000.0 + t1.tv_usec / 1000.0;
  }

  void clear() {
    for (int i = 0; i < 30; i++) {
      time[i] = 0.0;
      startCount[i] = 0;
      endCount[i] = 0;
    }
  }

  void reset() {
    for (int i = 0; i < 30; i++) {
      time[i] = timebase;
    }
  }
  
  void commonGetStartTime(int timeId) {
    struct timeval t1;                           
    gettimeofday(&t1, NULL);
    lock_guard<mutex> lock(timeMutex[timeId]);
    time[timeId] -= (t1.tv_sec * 1000.0 + t1.tv_usec / 1000.0) - timebase;
    startCount[timeId] += 1;
  }

  void commonGetEndTime(int timeId) {
    struct timeval t1;                           
    gettimeofday(&t1, NULL);
    lock_guard<mutex> lock(timeMutex[timeId]);
    time[timeId] += (t1.tv_sec * 1000.0 + t1.tv_usec / 1000.0) - timebase;
    endCount[timeId] += 1;
  }

  void quickGetStartTime(int *timeArray, int timeId) {
    struct timeval t1;
    gettimeofday(&t1, NULL);
    timeArray[timeId] -= (t1.tv_sec * 1000.0 + t1.tv_usec / 1000.0) - timebase;
  }

  void quickGetEndTime(int *timeArray, int timeId) {
    struct timeval t1;
    gettimeofday(&t1, NULL);
    timeArray[timeId] += (t1.tv_sec * 1000.0 + t1.tv_usec / 1000.0) - timebase;
  }

  double quickGetStartTime(double time) {
    struct timeval t1;
    gettimeofday(&t1, NULL);
    time -= (t1.tv_sec * 1000.0 + t1.tv_usec / 1000.0) - timebase;
    return time;
  }

  double quickGetEndTime(double time) {
    struct timeval t1;
    gettimeofday(&t1, NULL);
    time += (t1.tv_sec * 1000.0 + t1.tv_usec / 1000.0) - timebase;
    return time;
  }

  void fix() {
    for (int i = 0; i < 30; i++) {
      int exp = endCount[i] - startCount[i];
      if (exp > 0) {
        for (int j = 0; j < exp; j++) {
          time[i] += timebase;
        }
      }
      if (exp < 0) {
        cout << "[WARNING] start count greater than end count" << endl;
      }
    }
  }
  
  void showTime() {
    cout << endl;
    cout << "###########   Time  ##########" << endl;
    cout << "[Time] receive: ";
    cout << time[0] << endl;
    cout << "[Time] parse: ";
    cout << fixed << time[1] << endl;
    cout << "[Time] set: ";
    cout << time[2] << endl;
    cout << "[Time]  handle: ";
    cout << time[3] << endl;

    // set time
    cout << "[Time] total set: ";
    cout << time[5] << endl;
    cout << "[Time] cal hash: ";
    cout << fixed << time[6] << endl;
    cout << "[Time] cuckoo insert: ";
    cout << time[7] << endl;
    cout << "[Time] alloc item: ";
    cout << time[8] << endl;
    cout << "[Time] locate item: ";
    cout << time[9] << endl;
    cout << "[Time] set item: ";
    cout << time[10] << endl;
    // add new time counter here
    cout << "[Time] send set: ";
    cout << time[11] << endl;
    cout << "[Time] total get: ";
    cout << time[12] << endl;

    cout << "##############################" << endl;
    cout << endl;
  }

  void showCount() {
    cout << "###########   Time  ##########" << endl;
    cout << "[Time] receive: ";
    cout << startCount[0] << endl;
    cout << endCount[0] << endl;
    cout << "[Time] parse: ";
    cout << startCount[1] << endl;
    cout << endCount[1] << endl;
    cout << "[Time] set: ";
    cout << startCount[2] << endl;
    cout << endCount[2] << endl;
    cout << "[Time]  handle: ";
    cout << startCount[3] << endl;
    cout << endCount[3] << endl;

    // set time
    cout << "[Time] total set: ";
    cout << startCount[5] << endl;
    cout << endCount[5] << endl;
    cout << "[Time] cal hash: ";
    cout << startCount[6] << endl;
    cout << endCount[6] << endl;
    cout << "[Time] cuckoo insert: ";
    cout << startCount[7] << endl;
    cout << endCount[7] << endl;
    cout << "[Time] alloc item: ";
    cout << startCount[8] << endl;
    cout << endCount[8] << endl;
    cout << "[Time] locate item: ";
    cout << startCount[9] << endl;
    cout << endCount[9] << endl;
    cout << "[Time] set item: ";
    cout << startCount[10] << endl;
    cout << endCount[10] << endl;
    // add new time counter here
    cout << "[Time] send set: ";
    cout << startCount[11] << endl;
    cout << endCount[11] << endl;
    cout << "[Time] total get: ";
    cout << startCount[12] << endl;
    cout << endCount[12] << endl;

    cout << "[Time] 14: ";
    cout << startCount[14] << endl;
    cout << endCount[14] << endl;
    cout << "[Time] 15: ";
    cout << startCount[15] << endl;
    cout << endCount[15] << endl;

    cout << "##############################" << endl;
  }

};
