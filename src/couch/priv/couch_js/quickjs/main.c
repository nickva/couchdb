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

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "quickjs.h"
#include "quickjs-libc.h"

/* #ifdef XP_WIN */
/* #include <windows.h> */
/* #else */
/* #include <unistd.h> */
/* #endif */

#include "config.h"

#define SETUP_REQUEST(cx) \
    JS_SetContextThread(cx); \
    JS_BeginRequest(cx);
#define FINISH_REQUEST(cx) \
    JS_EndRequest(cx); \
    JS_ClearContextThread(cx);

typedef struct {
    int          eval;
    int          stack_size;
    const char** scripts;
} couch_args;


static JSValue
js_evalcx(JSContext* cx, JSValueConst this_val, int argc, JSValueConst *argv)
{
    jsval* argv = JS_ARGV(cx, vp);
    JSString* str;
    JSObject* sandbox;
    JSObject* global;
    JSContext* subcx;
    JSCrossCompartmentCall* call = NULL;
    const jschar* src;
    size_t srclen;
    jsval rval;
    JSBool ret = JS_FALSE;
    char *name = NULL;

    sandbox = NULL;
    if(!JS_ConvertArguments(cx, argc, argv, "S / o", &str, &sandbox)) {
        return JS_FALSE;
    }

    subcx = JS_NewContext(JS_GetRuntime(cx), 8L * 1024L);
    if(!subcx) {
        JS_ReportOutOfMemory(cx);
        return JS_FALSE;
    }

    SETUP_REQUEST(subcx);

    src = JS_GetStringCharsAndLength(cx, str, &srclen);

    // Re-use the compartment associated with the main context,
    // rather than creating a new compartment */
    global = JS_GetGlobalObject(cx);
    if(global == NULL) goto done;
    call = JS_EnterCrossCompartmentCall(subcx, global);

    if(!sandbox) {
        sandbox = JS_NewGlobalObject(subcx, &global_class);
        if(!sandbox || !JS_InitStandardClasses(subcx, sandbox)) {
            goto done;
        }
    }

    if(argc > 2) {
        name = JS_ToCString(cx, argv[2]);
    }

    if(srclen == 0) {
        JS_SET_RVAL(cx, vp, OBJECT_TO_JSVAL(sandbox));
    } else {
        JS_EvaluateUCScript(subcx, sandbox, src, srclen, name, 1, &rval);
        JS_SET_RVAL(cx, vp, rval);
    }

    ret = JS_TRUE;

done:
    if(name) JS_FreeCString(cx, name);
    JS_LeaveCrossCompartmentCall(call);
    FINISH_REQUEST(subcx);
    JS_DestroyContext(subcx);
    return ret;
}


static JSValue
js_gc(JSContext* cx, JSValueConst this_val, int argc, JSValueConst *argv)
{
    JS_RunGC(JS_GetRuntime(ctx));
    return JS_TRUE;
}


static JSValue
js_print(JSContext* cx, JSValueConst this_val, int argc, JSValueConst *argv)
{
    couch_print(cx, argc, argv);
    return JS_UNDEFINED;
}

static JSValue
js_quit(JSContext *ctx, JSValueConst this_val, int argc, JSValueConst *argv)
{
    int code;
    if (JS_ToInt32(ctx, &code, argv[0])) code = -1;
    exit(code);
    return JS_UNDEFINED;
}

static JSValue
js_readline(JSContext *ctx, JSValueConst this_val, int argc, JSValueConst *argv)
{
    /* GC Occasionally JS_MaybeGC(cx); */
    return couch_readline(cx, stdin);
}

static JSValue
js_seal(JSContext *ctx, JSValueConst this_val, int argc, JSValueConst *argv)
{
    jsval* argv = JS_ARGV(cx, vp);
    JSObject *target;
    JSBool deep = JS_FALSE;
    JSBool ret;

    if(!JS_ConvertArguments(cx, argc, argv, "o/b", &target, &deep))
        return JS_FALSE;

    if(!target) {
        JS_SET_RVAL(cx, vp, JSVAL_VOID);
        return JS_TRUE;
    }

    ret = deep ? JS_DeepFreezeObject(cx, target) : JS_FreezeObject(cx, target);
    JS_SET_RVAL(cx, vp, JSVAL_VOID);
    return ret;
}

static couch_args*
couch_parse_args(int argc, const char* argv[])
{
    couch_args* args;
    int i = 1;

    args = (couch_args*) malloc(sizeof(couch_args));
    if(args == NULL)
        return NULL;

    memset(args, '\0', sizeof(couch_args));
    args->stack_size = 64L * 1024L * 1024L;

    while(i < argc) {
        if(strcmp("-h", argv[i]) == 0) {
            DISPLAY_USAGE;
            exit(0);
        } else if(strcmp("-V", argv[i]) == 0) {
            DISPLAY_VERSION;
            exit(0);
        } else if(strcmp("-S", argv[i]) == 0) {
            args->stack_size = atoi(argv[++i]);
            if(args->stack_size <= 0) {
                fprintf(stderr, "Invalid stack size.\n");
                exit(2);
            }
        } else if(strcmp("--eval", argv[i]) == 0) {
            args->eval = 1;
        } else if(strcmp("--", argv[i]) == 0) {
            i++;
            break;
        } else {
            break;
        }
        i++;
    }

    if(i >= argc) {
        DISPLAY_USAGE;
        exit(3);
    }
    args->scripts = argv + i;

    return args;
}


static int
couch_fgets(char* buf, int size, FILE* fp)
{
    int n, i, c;

    if(size <= 0) return -1;
    n = size - 1;

    for(i = 0; i < n && (c = getc(fp)) != EOF; i++) {
        buf[i] = c;
        if(c == '\n') {
            i++;
            break;
        }
    }

    buf[i] = '\0';
    return i;
}


static JSValue
couch_readline(JSContext* cx, FILE* fp)
{
    JSValue str;
    char* bytes = NULL;
    char* tmp = NULL;
    size_t used = 0;
    size_t byteslen = 256;
    size_t readlen = 0;

    bytes = js_malloc(cx, byteslen);
    if(!bytes) {
        return JS_EXCEPTION;
    }

    while((readlen = couch_fgets(bytes+used, byteslen-used, fp)) > 0) {
        used += readlen;

        if(bytes[used-1] == '\n') {
            bytes[used-1] = '\0';
            break;
        }

        // Double our buffer and read more.
        byteslen *= 2;
        tmp = js_realloc(cx, bytes, byteslen);
        if(!tmp) {
            js_free(cx, bytes);
            return JS_EXCEPTION;
        }
        bytes = tmp;
    }

    // Treat empty strings specially
    if(used == 0) {
        js_free(cx, bytes);
        return JS_NewString(cx, "");
    }

    str = JSNewStringLen(cx, bytes, byteslen);
    js_free(cx, bytes);
    return str;
}


static int
couch_eval_file(JSContext * cx, const char* filename)
{
    uint8_t* buf;
    const char* filename;
    JSValue val;
    size_t buf_len;
    int ret;

    // load into buf and set buf_len to length
    buf = js_load_file(NULL, &buf_len, filename);
    if(!buf) {
        perror(filename);
        exit(1);
    }
    // evaluate
    val = JS_Eval(ctx, (char* )buf, buf_len, filename, JS_EVAL_TYPE_GLOBAL);
    if (JS_IsException(val)) {
        js_std_dump_error(ctx);
        ret = -1;
    } else {
        ret = 0
    }
    free(buf);
    JS_FreeValue(ctx, val);
    return ret;
}

static void
couch_print(JSContext* cx, uintN argc, jsval* argv)
{
    const char *bytes;
    FILE *stream = stdout;

    if (argc) {
        if (argc > 1 && JS_VALUE_GET_BOOL(argv[1])) {
          stream = stderr;
        }
        bytes = JS_ToCString(cx, argv[0]);
        if(!bytes) return;
        fputs(bytes, stream);
        JS_FreeCString(cx, bytes);
    }

    fputc('\n', stream);
    fflush(stream);
}

static const JSCFunctionListEntry globals[] = {
   JS_CFUNC_DEF("evalcx", 2, js_evalcx),
   JS_CFUNC_DEF("gc", 0, js_gc),
   JS_CFUNC_DEF("quit", 1, js_quit),
   JS_CFUNC_DEF("print", 1, js_print),
   JS_CFUNC_DEF("readline", 0, js_readline),
   JS_CFUNC_DEF("seal", 2, js_seal)
}

int
main(int argc, const char* argv[])
{
    JSRuntime* rt = NULL;
    JSContext* cx = NULL;
    JSObject* global = NULL;
    JSCrossCompartmentCall *call = NULL;
    JSSCRIPT_TYPE script;
    JSString* scriptsrc;
    const jschar* schars;
    size_t slen;
    jsval sroot;
    JSValue result;
    int i;

    couch_args* args = couch_parse_args(argc, argv);

    rt = JS_NewRuntime(args->stack_size);
    if(rt == NULL)
        return 1;

    cx = JS_NewContext(rt, 8L * 1024L);
    if(cx == NULL)
        return 1;

    JS_SetContextPrivate(cx, args);

    SETUP_REQUEST(cx);

    global = JS_NewCompartmentAndGlobalObject(cx, &global_class, NULL);
    if(global == NULL)
        return 1;

    call = JS_EnterCrossCompartmentCall(cx, global);

    JS_SetGlobalObject(cx, global);

    if(!JS_InitStandardClasses(cx, global))
        return 1;

    JS_SetPropertyFunctionList(ctx, ctx->global_obj, globals, countof(globals));

    for(i = 0 ; args->scripts[i] ; i++) {
        if (couch_eval_file(cx, args->scripts[i]))
            return 1;
    }

    JS_LeaveCrossCompartmentCall(call);
    FINISH_REQUEST(cx);
    JS_DestroyContext(cx);
    JS_DestroyRuntime(rt);
    JS_ShutDown();

    return 0;
}
