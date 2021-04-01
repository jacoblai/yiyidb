#include <unistd.h>
#include "ticker.h"
#include "_cgo_export.h"

void cticker()
{
  for(int i = 0; i <= 10; i++)
  {
    Gotask();
    usleep(1000000);
    if (i>= 10){
       i = 0;
    }
  }
}