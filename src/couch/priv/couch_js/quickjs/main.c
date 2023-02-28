// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include "quickjs.h"

int main(void) {
  clock_t t0, t1, t2;

  t0 = clock();

  JSRuntime *rt = JS_NewRuntime();
  JS_SetMemoryLimit(rt, 64L * 1024L * 1024L);

  JSContext *ctx = JS_NewContext(rt);

  t1 = clock();

  JS_FreeContext(ctx);
  JS_FreeRuntime(rt);

  t2 = clock();

  printf("\nInit (usec): \t%f\n", (double)(t1 - t0) * 1e6 / CLOCKS_PER_SEC);
  printf("Free (usec): \t%f\n",   (double)(t2 - t1) * 1e6 / CLOCKS_PER_SEC);

  return 0;
}
