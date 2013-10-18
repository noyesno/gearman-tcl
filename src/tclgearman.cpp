/**************************************************************
* Gearman Tcl Client/Worker/Admin
* -------------------------------------------------------------
* by: Sean Zhang
: at: Oct, 2013
***************************************************************/
// Ref: http://gearman.org/protocol

#include "../generic/dbg.h"
#include <libgearman/gearman.h>

#include <tcl.h>
#include <stdio.h>
#include <iostream>


// TODO: allow multiple instance!!!
extern "C" {
  gearman_client_st client;
  gearman_worker_st *worker;
  Tcl_Channel       admin;
}


///////////////////////////////////////////////////////////////////////
// Client                                                            //
///////////////////////////////////////////////////////////////////////
struct tcl_client_context_t {
  Tcl_Interp *interp;
  Tcl_Obj    *callback;
  //std::string task_proc;
};
tcl_client_context_t tcl_client_context;

static int tcl_gearman_client_data(gearman_task_st *task){
  int ret;
  ret = Tcl_GlobalEvalObj(tcl_client_context.interp, tcl_client_context.callback);

  return TCL_OK;
}


int TclObjCmd_gearman_client_create(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *const objv[]) {

  gearman_client_st &client = *(gearman_client_st *)clientData;

  // TODO: use gearman_client_create(NULL)
  if (gearman_client_create(&client) == NULL){
    debug_error("Memory allocation failure on client creation");
    return TCL_ERROR;
  }

  const char *host = NULL;
  in_port_t port = GEARMAN_DEFAULT_TCP_PORT; // :4730

  if(objc>2){
    host = Tcl_GetString(objv[2]);
  }else if(getenv("GEARMAN_SERVER")){
    host = getenv("GEARMAN_SERVER");
  }else{
    host="localhost";
  }

  gearman_return_t ret;
  // ret= gearman_client_add_server(&client, host, port);
  ret = gearman_client_add_servers(&client, host);  // e.g. host1:4730,host2:4730
  if (ret != GEARMAN_SUCCESS) {
    log_error("Gearman add server FAIL: %s", host);
    debug_error("%s", gearman_client_error(&client));
    return TCL_ERROR;
  }

  //-- static int idx = 0;
  //-- int argc = 1;
  //-- const char *argv[] = {"id"};
  //-- Tcl_CreateAlias(interp, "", interp, "gearman", argc, argv);

  //-- Tcl_Obj *client_idx = Tcl_NewIntObj(++idx);

  //-- int _objc = 1;
  //-- Tcl_Obj *_objv[_objc];
  //-- _objv[0] = client_idx;

  //-- Tcl_HashTable table;
  //-- Tcl_InitObjHashTable(&table);
  //-- int newPtr = 0;
  //-- Tcl_HashEntry *record = Tcl_CreateHashEntry(&table, client_idx, &newPtr);
  //-- Tcl_SetHashValue(record, &client);
  //--

  //-- Tcl_Obj    *client_idx = Tcl_NewIntObj(1);
  //-- uintptr_t number = (uintptr_t) client_idx; //PTR2INT(client_idx);

  int   client_idx = 1;
  char client_cmd[256];
  sprintf(client_cmd, "gearman::client@%d", client_idx);

  int _objc = 1;
  Tcl_Obj *_objv[_objc];
  _objv[0] = Tcl_NewIntObj(client_idx);
  Tcl_CreateAliasObj(interp, client_cmd, interp, "gearman::client", _objc, _objv);

  Tcl_SetObjResult(interp, Tcl_NewStringObj(client_cmd, -1));

  return TCL_OK;
}

int TclObjCmd_gearman_client_config(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *const objv[]) {
  // gearman::client $client config -client_id $value
  // objv[1] = $client
  // objv[2] = config
  // objv[3] = key
  // objv[4] = value
  for(int j=4; j<objc; j+=2){
    const char *name  = Tcl_GetString(objv[j-1]);
    if(0==strcmp("-client_id",name) || 0==strcmp("-id",name)){
      int size = 0;
      const char *value = Tcl_GetStringFromObj(objv[j], &size);
      debug_info("client config -client_id %s", value);
      if (gearman_failed(gearman_client_set_identifier(&client, value, size))){
        std::cerr << gearman_worker_error(worker) << std::endl;
        return TCL_ERROR;
      }
      //return TCL_OK;
    }else if(0==strcmp("-timeout",name)){
      int timeout = 0;
      Tcl_GetIntFromObj(interp, objv[j], &timeout);
      gearman_client_set_timeout(&client, timeout);
      //return TCL_OK;
    }else if(0==strcmp("-namespace",name)){
      int size = 0;
      const char *value = Tcl_GetStringFromObj(objv[j], &size);
      gearman_client_set_namespace(&client, value, size); 
    }else{
      debug_info("Unsupported client config %s", name);
      return TCL_ERROR;
    }
  }
  return TCL_OK;
}

static gearman_return_t cbk_client_complete(gearman_task_st *task){
  debug_info("client task complete");
  const char *function = gearman_task_function_name(task);
  debug_info("function %s :", function);
  write(fileno(stdout), gearman_task_data(task), gearman_task_data_size(task));
  return GEARMAN_SUCCESS;
}
static gearman_return_t cbk_client_created(gearman_task_st *task){
  debug_info("client task created");
  return GEARMAN_SUCCESS;
}

static gearman_return_t cbk_client_tcl(gearman_task_st *task){
  debug_info("client task tcl");
  return GEARMAN_SUCCESS;
}
static gearman_return_t cbk_client_status(gearman_task_st *task){
  debug_info("client task status %d / %d", gearman_task_numerator(task), gearman_task_denominator(task));
  return GEARMAN_SUCCESS;
}

static gearman_return_t cbk_client_data(gearman_task_st *task){
  debug_info("inside callback");
  /*
  gearman_task_job_handle(task)
  gearman_task_context(task)
  */
  const char *function = gearman_task_function_name(task);
  debug_info("function %s :", function);
  write(fileno(stdout), gearman_task_data(task), gearman_task_data_size(task));
  return GEARMAN_SUCCESS;
}

int TclObjCmd_gearman_client_callback(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *const objv[]) {
  // objv[] = {gearman::client $client callback -complete complete_fn ...}
  gearman_client_st &client = *(gearman_client_st *)clientData;
  gearman_return_t ret;

  for(int i=3; i<objc-1; i+=2){
    const char *name  = Tcl_GetString(objv[i]);
    const char *value = Tcl_GetString(objv[i+1]);
    if(0==strcmp("-complete", name)){
      gearman_client_set_complete_fn(&client, cbk_client_complete);
    }else if(0==strcmp("-created", name)){
      gearman_client_set_created_fn(&client, cbk_client_created);
    }else if(0==strcmp("-data", name)){
      debug_info("add callback %s", name);
      gearman_client_set_data_fn(&client, cbk_client_data);
    }else if(0==strcmp("-warning", name)){
      gearman_client_set_warning_fn(&client, cbk_client_tcl);
    }else if(0==strcmp("-status", name)){
      gearman_client_set_status_fn(&client, cbk_client_status);
    }else if(0==strcmp("-exception", name)){
      gearman_client_set_status_fn(&client, cbk_client_tcl);
    }else if(0==strcmp("-fail", name)){
      gearman_client_set_status_fn(&client, cbk_client_tcl);
    }else if(0==strcmp("-clear", name)){
      gearman_client_set_status_fn(&client, cbk_client_tcl);
    }
  }
  return TCL_OK;
}

int TclObjCmd_gearman_client_run(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *const objv[]) {
  gearman_client_st &client = *(gearman_client_st *)clientData;
  gearman_return_t ret;

  ret = gearman_client_run_tasks(&client);
  if(GEARMAN_PAUSE==ret){
    return TCL_ERROR;
  }else if(GEARMAN_SUCCESS!=ret){
    log_error("fail to gearman_client_run_tasks %s", gearman_client_error(&client));
    return TCL_ERROR;
  }
  debug_info("running gearman_client_run_tasks");
  return TCL_OK;
}

int TclObjCmd_gearman_client_addtask(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *const objv[]) {
  // objv[] = {gearman::clint $client addtask -complete complete_fn ...}
  gearman_client_st &client = *(gearman_client_st *)clientData;
  gearman_return_t ret;

  bool        task_background = false;
  int         task_priority = GEARMAN_JOB_PRIORITY_NORMAL;
  const char *task_unique  = NULL;
  for(int i=3; i<objc-2; i++){
    if(0==strcmp("-background", Tcl_GetString(objv[i]))){
      task_background = true;
    }else if(0==strcmp("-high", Tcl_GetString(objv[i]))){
      task_priority = GEARMAN_JOB_PRIORITY_HIGH;
    }else if(0==strcmp("-low", Tcl_GetString(objv[i]))){
      task_priority = GEARMAN_JOB_PRIORITY_LOW;
    }else if(0==strcmp("-uuid", Tcl_GetString(objv[i]))){
      task_unique = Tcl_GetString(objv[++i]);
    }
  }

  const char *command  = Tcl_GetString(objv[objc-2]);
  int   workload_size = -1;
  const char *workload = Tcl_GetStringFromObj(objv[objc-1], &workload_size);

  void *context = NULL;
  gearman_task_st *task = NULL;
  const char *unique = task_unique;

  if(task_background){
    switch(task_priority){
      case GEARMAN_JOB_PRIORITY_NORMAL  :
        gearman_client_add_task_background(&client, task, context, command, task_unique, workload, workload_size, &ret);
        break;
      case GEARMAN_JOB_PRIORITY_HIGH :
        gearman_client_add_task_high_background(&client, task, context, command, task_unique, workload, workload_size, &ret);
        break;
      case GEARMAN_JOB_PRIORITY_LOW  :
        gearman_client_add_task_low_background(&client, task, context, command, task_unique, workload, workload_size, &ret);
        break;
    }
  } else {
    switch(task_priority){
      case GEARMAN_JOB_PRIORITY_NORMAL  :
        debug_info("add task %s %s", command, workload);
        gearman_client_add_task(&client, task, context, command, unique, workload, workload_size, &ret);
        break;
      case GEARMAN_JOB_PRIORITY_HIGH :
        gearman_client_add_task_high(&client, task, context, command, unique, workload, workload_size, &ret);
        break;
      case GEARMAN_JOB_PRIORITY_LOW  :
        gearman_client_add_task_low(&client, task, context, command, unique, workload, workload_size, &ret);
        break;
    }
  }

  if(gearman_failed(ret)){
     log_error("fail to gearman_client_add_task");
     return TCL_ERROR;
  }
  return TCL_OK;
}

int TclObjCmd_gearman_client_submit(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *const objv[]) {
  gearman_client_st &client = *(gearman_client_st *)clientData;
  gearman_return_t ret;

  bool        task_background = false;
  int         task_priority = GEARMAN_JOB_PRIORITY_NORMAL;
  const char *task_unique  = NULL;
  for(int i=3; i<objc-2; i++){
    if(0==strcmp("-background", Tcl_GetString(objv[i]))){
      task_background = true;
    }else if(0==strcmp("-high", Tcl_GetString(objv[i]))){
      task_priority = GEARMAN_JOB_PRIORITY_HIGH;
    }else if(0==strcmp("-low", Tcl_GetString(objv[i]))){
      task_priority = GEARMAN_JOB_PRIORITY_LOW;
    }else if(0==strcmp("-uuid", Tcl_GetString(objv[i]))){
      task_unique = Tcl_GetString(objv[++i]);
    }
  }
  const char *command  = Tcl_GetString(objv[objc-2]);
  int   workload_size = -1;
  const char *workload = Tcl_GetStringFromObj(objv[objc-1], &workload_size);

  void *context = NULL;
  gearman_task_st *task = NULL;

  size_t result_size;
  char *result;

  debug("%s < %s ;# %s %s", command, workload, Tcl_GetString(objv[0]), Tcl_GetString(objv[1]));

  if(task_background){
    gearman_job_handle_t job_handle; // char job_handle[256];
    switch(task_priority){
      case GEARMAN_JOB_PRIORITY_NORMAL  :
        ret = gearman_client_do_background(&client, command, task_unique, workload, workload_size, job_handle);
        break;
      case GEARMAN_JOB_PRIORITY_HIGH :
        ret = gearman_client_do_high_background(&client, command, task_unique, workload, workload_size, job_handle);
        break;
      case GEARMAN_JOB_PRIORITY_LOW  :
        ret = gearman_client_do_low_background(&client, command, task_unique, workload, workload_size, job_handle);
        break;
    }
    Tcl_SetObjResult(interp, Tcl_NewStringObj(job_handle, -1));
    return TCL_OK;
  } else {
    switch(task_priority){
      case GEARMAN_JOB_PRIORITY_NORMAL  :
        result = (char *)gearman_client_do(&client, command, task_unique, workload, workload_size, &result_size, &ret);
        break;
      case GEARMAN_JOB_PRIORITY_HIGH :
        result = (char *)gearman_client_do_high(&client, command, task_unique, workload, workload_size, &result_size, &ret);
        break;
      case GEARMAN_JOB_PRIORITY_LOW  :
        result = (char *)gearman_client_do_low(&client, command, task_unique, workload, workload_size, &result_size, &ret);
        break;
    }
  }

  if (ret == GEARMAN_WORK_DATA) {
      debug("GEARMAN_WORK_DATA");
      std::cout.write(result, result_size);
      free(result); // MUST free the value
      return TCL_OK;
  } else if (ret == GEARMAN_WORK_STATUS) {
      uint32_t numerator;
      uint32_t denominator;

      gearman_client_do_status(&client, &numerator, &denominator);
      std::clog << "Status: " << numerator << "/" << denominator << std::endl;

      return TCL_OK;
  } else if (ret == GEARMAN_SUCCESS) {
      // std::cout.write(result, result_size);
      Tcl_SetObjResult(interp, Tcl_NewStringObj(result, result_size));
      free(result); // MUST free the value
      return TCL_OK;
  } else if (ret == GEARMAN_WORK_FAIL) {
      debug_error("GEARMAN_WORK_FAIL");
      return TCL_ERROR;
  } else if (ret == GEARMAN_COULD_NOT_CONNECT) {
      debug_error("Can not connect to server %s", gearman_client_error(&client));
      return TCL_ERROR;
  } else if (ret == GEARMAN_PAUSE) {
      log_error("GEARMAN_PAUSE found");
      return TCL_ERROR;
  } else {
      debug_error("%s", gearman_client_error(&client));
      return TCL_ERROR;
  }

  return TCL_OK;
}


int TclObjCmd_gearman_client(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *const objv[]) {
  gearman_client_st &client = *(gearman_client_st *)clientData;
  debug_info("client | %s %s", Tcl_GetString(objv[0]), Tcl_GetString(objv[1]));

  const char *subcmd = Tcl_GetString(objv[1]);
  if(0==strcmp("create", subcmd)) {
    return TclObjCmd_gearman_client_create(clientData, interp, objc, objv);
  }

  const char *actcmd = Tcl_GetString(objv[2]);
  debug_info("client actcmd = %s", actcmd);
  if(0==strcmp("submit", actcmd)) {
    return TclObjCmd_gearman_client_submit(clientData, interp, objc, objv);
  }else if(0==strcmp("config", actcmd)) {
    return TclObjCmd_gearman_client_config(clientData, interp, objc, objv);
  }else if(0==strcmp("callback", actcmd)) {
    return TclObjCmd_gearman_client_callback(clientData, interp, objc, objv);
  }else if(0==strcmp("addtask", actcmd)) {
    return TclObjCmd_gearman_client_addtask(clientData, interp, objc, objv);
  }else if(0==strcmp("run", actcmd)) {
    return TclObjCmd_gearman_client_run(clientData, interp, objc, objv);
  }else if(0==strcmp("close", actcmd)) {
    gearman_client_free(&client);
    return TCL_OK;
  } else {
    Tcl_AddErrorInfo(interp, "Unsupported sub command");
    Tcl_SetErrorCode(interp, actcmd,"123", NULL);
    return TCL_ERROR;
  }
  return TCL_OK;
}

///////////////////////////////////////////////////////////////////////
// Worker                                                            //
///////////////////////////////////////////////////////////////////////
struct tcl_worker_context_t {
  Tcl_Interp *interp;
  Tcl_Obj    *worker;
  //std::string task_proc;
};
tcl_worker_context_t tcl_worker_context;

static gearman_return_t gearman_tcl_worker(gearman_job_st *job, void *context) {
  const char *workload= (const char *)gearman_job_workload(job);
  const size_t workload_size= gearman_job_workload_size(job);

  debug_info("workload_size = %d", workload_size);

  int      objc = 3;
  Tcl_Obj *objv[objc];
  objv[0] = (Tcl_Obj *) context;
  objv[1] = tcl_worker_context.worker;                  // $worker
  objv[2] = Tcl_NewStringObj(workload, workload_size);  // $data
  Tcl_Interp *interp = tcl_worker_context.interp;
  if(TCL_OK != Tcl_EvalObjv(interp, objc, objv, TCL_EVAL_GLOBAL)){
    debug_error("Fail to eval %s", Tcl_GetString(objv[0]));
    return GEARMAN_FAIL;
  }

  int result_size = 0;
  const char *result = Tcl_GetStringFromObj(Tcl_GetObjResult(interp), &result_size);

  if (gearman_failed(gearman_job_send_status(job, (uint32_t)0, (uint32_t)result_size))){
    return GEARMAN_FAIL;
  }
  if (gearman_failed(gearman_job_send_data(job, result, result_size))){
    return GEARMAN_FAIL;
  }
      // Notice that we send based on y divided by zero.
  if (gearman_failed(gearman_job_send_status(job, (uint32_t)result_size, (uint32_t)result_size))) {
    return GEARMAN_FAIL;
  }

  return GEARMAN_SUCCESS;
}


int TclObjCmd_gearman_worker_create(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *const objv[]) {
  if ((worker= gearman_worker_create(NULL)) == NULL){
    debug_error("Memory allocation failure on worker creation");
    return TCL_ERROR;
  }

  gearman_worker_add_options(worker, GEARMAN_WORKER_GRAB_UNIQ);

  // objv[1] == "create"
  const char *host = Tcl_GetString(objv[2]);
  in_port_t port = GEARMAN_DEFAULT_TCP_PORT;

  if(objc>2){
    host = Tcl_GetString(objv[2]);
  }else if(getenv("GEARMAN_SERVER")){
    host = getenv("GEARMAN_SERVER");
  }else{
    host="localhost";
  }

  if (gearman_failed(gearman_worker_add_servers(worker, host))) {
    std::cerr << gearman_worker_error(worker) << std::endl;
    return TCL_ERROR;
  }



  int   worker_idx = 1;
  char  worker_cmd[256];
  sprintf(worker_cmd, "gearman::worker@%d", worker_idx);

  int _objc = 1;
  Tcl_Obj *_objv[_objc];
  //_objv[0] = Tcl_NewIntObj(worker_idx);
  _objv[0] = Tcl_NewStringObj(worker_cmd,-1);
  Tcl_CreateAliasObj(interp, worker_cmd, interp, "gearman::worker", _objc, _objv);

  Tcl_SetObjResult(interp, Tcl_NewStringObj(worker_cmd, -1));

  return TCL_OK;
}

int TclObjCmd_gearman_worker_config(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *const objv[]) {
  // objv[1] = $worker
  // objv[2] = config
  // objv[3] = key
  for(int j=4; j<objc; j+=2){
    const char *name  = Tcl_GetString(objv[j-1]);
    debug_info("worker config %s = %s", Tcl_GetString(objv[j-1]), Tcl_GetString(objv[j]));
    if(0==strcmp("-worker_id",name) || 0==strcmp("-id",name)){
      int size = 0;
      const char *value = Tcl_GetStringFromObj(objv[j], &size);
      if (gearman_failed(gearman_worker_set_identifier(worker, value, size))){
        std::cerr << gearman_worker_error(worker) << std::endl;
        return TCL_ERROR;
      }
      // return TCL_OK;
    }else if(0==strcmp("-timeout",name)){
      int timeout = 0;
      Tcl_GetIntFromObj(interp, objv[j], &timeout);
      gearman_worker_set_timeout(worker, timeout);
      // return TCL_OK;
    }else if(0==strcmp("-namespace",name)){
      int size = 0;
      const char *value = Tcl_GetStringFromObj(objv[j], &size);
      gearman_worker_set_namespace(worker, value, size);
    }else if(0==strcmp("-blocking",name)){
      int blocking = 1;
      Tcl_GetBooleanFromObj(interp, objv[j], &blocking);
      if(blocking){
        gearman_worker_remove_options(worker, GEARMAN_WORKER_NON_BLOCKING);
      }else{
        gearman_worker_add_options(worker, GEARMAN_WORKER_NON_BLOCKING);
      }
    }else{
      debug_info("Unsupported worker config %s", name);
      return TCL_ERROR;
    }
  }
  return TCL_OK;
}

int Tcl_gearman_worker_register_task(gearman_worker_st *worker,
   const char *task_name, const char *task_proc=NULL)
{

  if(task_name==NULL){
    debug_info("unregiser all functions");
    gearman_worker_unregister_all(worker);
    return TCL_OK;
  }

  if(task_proc==NULL){
    debug_info("unregiser function %s", task_name);
    gearman_worker_unregister(worker, task_name);
    return TCL_OK;
  }
  // register function
  gearman_function_t worker_cb = gearman_function_create(gearman_tcl_worker);


  //-- if (gearman_failed(gearman_worker_add_function(&worker, task_name, 0, worker_cb, NULL))){
  //--   // ...
  //-- }

  if (gearman_failed(gearman_worker_define_function(worker,
                                                    task_name, strlen(task_name),
                                                    worker_cb,
                                                    0,
                                                    Tcl_NewStringObj(task_proc,-1))))
  {
    debug_error("Fail to regiser service %s as %s", task_name, task_proc);
    std::cerr << gearman_worker_error(worker) << std::endl;
    return TCL_ERROR;
  }
  debug_info("OK to regiser service %s as %s", task_name, task_proc);
  return TCL_OK;
}

// extern void gearman_nap(int arg);

void gearman_nap(int arg) {
  if (arg < 1)
  { }
  else
  {
#ifdef WIN32
    sleep(arg/1000000);
#else
    struct timespec global_sleep_value= { 0, static_cast<long>(arg * 1000)};
    nanosleep(&global_sleep_value, NULL);
#endif
  }
}


inline int Tcl_gearman_worker_work_callback(Tcl_Interp *interp, gearman_worker_st *worker, Tcl_Obj *callback){
  if(callback==NULL) return TCL_OK;

  Tcl_GlobalEvalObj(interp, callback);
  int go = 1;
  Tcl_GetBooleanFromObj(interp, Tcl_GetObjResult(interp), &go);
  if(!go){
    return TCL_BREAK;
  }
  return TCL_OK;
}

int Tcl_gearman_worker_work(Tcl_Interp *interp, gearman_worker_st *worker, Tcl_Obj *callback=NULL){
  // work infinit
  // GEARMAN_WORKER_TIMEOUT_RETURN
  while(1){
    debug_info("work");
    gearman_return_t ret = gearman_worker_work(worker);

    if(GEARMAN_SUCCESS == ret){
      int ret = Tcl_gearman_worker_work_callback(interp, worker, callback);
      if(TCL_BREAK == ret){
        break;
      }else if(TCL_OK != ret){
        return ret;
      }
      continue;
    }else if(GEARMAN_IO_WAIT == ret || GEARMAN_NO_JOBS == ret){
      debug_info("worker timeout = %d",gearman_worker_timeout(worker));
      gearman_nap(10*1000); // in us #define GEARMAN_WORKER_WAIT_TIMEOUT (10 * 1000) /* Milliseconds */
      // callback
      int ret = Tcl_gearman_worker_work_callback(interp, worker, callback);
      if(TCL_BREAK == ret){
        break;
      }else if(TCL_OK != ret){
        return ret;
      }
      continue;
    }else if(GEARMAN_NO_ACTIVE_FDS == ret){   // TODO: gearman_worker_wait(worker);
      log_info("GEARMAN_NO_ACTIVE_FDS found");
      sleep(5);
      // callback
      continue;
    }else if(gearman_failed(ret)){
      debug_error("work fail %d %s", ret, gearman_worker_error(worker));
      break;
    }
  }
  return TCL_OK;
}

int TclObjCmd_gearman_worker(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *const objv[]) {
  //gearman_worker_st *worker = (gearman_worker_st *)clientData;

  debug_info("worker | %s %s", Tcl_GetString(objv[0]), Tcl_GetString(objv[1]));

  const char *subcmd = Tcl_GetString(objv[1]);
  if(0==strcmp("create", subcmd)) {
    return TclObjCmd_gearman_worker_create(clientData, interp, objc, objv);
  }

  // objv[1] = worker
  const char *actcmd = Tcl_GetString(objv[2]);

  debug_info("actcmd = %s", actcmd);
  if(0==strcmp("register", actcmd)) {
    const char *task_name = Tcl_GetString(objv[3]);
    const char *task_proc = Tcl_GetString(objv[4]);
    return Tcl_gearman_worker_register_task(worker, task_name, task_proc);
  }else if(0==strcmp("unregister", actcmd)) {
    const char *task_name = Tcl_GetString(objv[3]);
    if(0==strcmp("-all", task_name)){
      return Tcl_gearman_worker_register_task(worker, NULL, NULL);
    }else{
      return Tcl_gearman_worker_register_task(worker, task_name, NULL);
    }
  }else if(0==strcmp("work", actcmd)) {
    tcl_worker_context.worker = objv[1];
    tcl_worker_context.interp = interp;
    Tcl_Obj *callback = NULL;
    if(objc>3){
      callback = objv[3];
    }
    return Tcl_gearman_worker_work(interp, worker, callback);
  }else if(0==strcmp("config", actcmd)) {
    return TclObjCmd_gearman_worker_config(clientData, interp, objc, objv);
  }else if(0==strcmp("close", actcmd)) {
    gearman_worker_free(worker);
    return TCL_OK;
  } else {
    log_error("Error: unknown actcmd = %s", actcmd);
    return TCL_ERROR;
  }
  return TCL_OK;
}

int TclObjCmd_gearman(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *const objv[]) {
  static int client_id = 0;
  gearman_client_st &client = *(gearman_client_st *)clientData;
  const char *subcmd = Tcl_GetString(objv[1]);
  if(0==strcmp("client", subcmd)) {
    return TclObjCmd_gearman_client_create(clientData, interp, objc-1, objv+1);
  } else if(0==strcmp("worker", subcmd)) {
    return TclObjCmd_gearman_worker_create(clientData, interp, objc, objv);
  } else {
  }
  return TCL_OK;
}

int TclObjCmd_gearman_admin_create(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *const objv[]) {

  const char *host = Tcl_GetString(objv[2]);
  in_port_t port = GEARMAN_DEFAULT_TCP_PORT;

  debug_info("connect to %s:%d ...", host, port);
  Tcl_Channel sock = Tcl_OpenTcpClient(interp, port, host, NULL, 0, 0);
  if(sock==NULL){
    log_error("connect to %s:%d fail", host, port);
    return TCL_ERROR;
  }

  admin = sock; // TOOD
  //Tcl_GetChannelOption(interp, sock, optionName, optionValue);
  Tcl_SetChannelOption(interp, sock, "-buffering", "line");

  // interp alias ...
  int  admin_idx = 1;
  char admin_cmd[256];
  sprintf(admin_cmd, "gearman::admin@%d", admin_idx);

  int _objc = 1;
  Tcl_Obj *_objv[_objc];
  _objv[0] = Tcl_NewIntObj(admin_idx);
  Tcl_CreateAliasObj(interp, admin_cmd, interp, "gearman::admin", _objc, _objv);

  Tcl_SetObjResult(interp, Tcl_NewStringObj(admin_cmd, -1));
  return TCL_OK;
}

Tcl_Obj *read_sock_reply(Tcl_Interp *interp, Tcl_Channel sock, const char *command){
    int n_puts = Tcl_WriteChars(sock,command,-1);
    Tcl_WriteChars(sock,"\n",-1);
    // Tcl_Flush(sock);
    Tcl_Obj *lineObj = Tcl_NewObj();
    int size = Tcl_GetsObj(sock, lineObj);
    debug_info("%s", Tcl_GetString(lineObj));

    Tcl_Obj *value = Tcl_NewObj();
    Tcl_ListObjIndex(interp, lineObj, 1, &value);
    Tcl_IncrRefCount(value);
    Tcl_DecrRefCount(lineObj);
    return value;
}

int TclObjCmd_gearman_admin(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *const objv[]) {

  const char *subcmd = Tcl_GetString(objv[1]);
  if(0==strcmp("create", subcmd)) {
    return TclObjCmd_gearman_admin_create(clientData, interp, objc, objv);
  }

  const char *actcmd = Tcl_GetString(objv[2]);
  Tcl_Channel sock = admin;
  if(0==strcmp("close", actcmd)){
    debug_info("close");
    if(TCL_OK != Tcl_Close(interp, sock)){
      int errorCode = Tcl_GetErrno();
      log_error("(%d:%s) %s", errorCode, Tcl_ErrnoId(), Tcl_ErrnoMsg(errorCode));
      return TCL_ERROR;
    }
    return TCL_OK;
  }else if(0==strcmp("version", actcmd)) {                // version
    Tcl_Obj *result = read_sock_reply(interp, sock, actcmd);
    Tcl_SetObjResult(interp, result);
    return TCL_OK;
  }else if(0==strcmp("status", actcmd)) {                 // status
    /* Format: FUNCTION\tTOTAL\tRUNNING\tAVAILABLE_WORKERS */
    int n_puts = Tcl_WriteChars(sock,"status\n",-1);
    Tcl_Obj *list = Tcl_NewListObj(0, NULL);
    while(1){
      Tcl_Obj *lineObj = Tcl_NewObj();
      int size = Tcl_GetsObj(sock, lineObj);
      if(0==strcmp(".",Tcl_GetString(lineObj))){
        break;
      }
      Tcl_ListObjAppendElement(interp, list, lineObj);
      debug_info("%s", Tcl_GetString(lineObj));
    }
    Tcl_SetObjResult(interp,list);
    return TCL_OK;
  }else if(0==strcmp("workers", actcmd)) {                // workers
    /* Format: FD IP-ADDRESS CLIENT-ID : FUNCTION ... */
    int n_puts = Tcl_WriteChars(sock,"workers\n",-1);
    Tcl_Obj *list = Tcl_NewListObj(0, NULL);
    while(1){
      Tcl_Obj *lineObj = Tcl_NewObj();
      int size = Tcl_GetsObj(sock, lineObj);

      if(0==strcmp(".",Tcl_GetString(lineObj))){
        break;
      }
      Tcl_ListObjAppendElement(interp, list, lineObj);
      debug_info("%s", Tcl_GetString(lineObj));
    }
    Tcl_SetObjResult(interp,list);
    return TCL_OK;
  }else if(0==strcmp("maxqueue", actcmd)) {   // maxqueue $function <$size>
    return TCL_OK;
  }else if(0==strcmp("shutdown", actcmd)) {   // shutdown
    return TCL_OK;
  }else if(0==strcmp("verbose", actcmd)) {    // verbose
    Tcl_Obj *result = read_sock_reply(interp, sock, actcmd);
    Tcl_SetObjResult(interp, result);
    return TCL_OK;
  }else if(0==strcmp("cancel job", actcmd)) {     // cancel job $id
    return TCL_OK;
  }else if(0==strcmp("show unique jobs", actcmd)) {     // show unique jobs
    return TCL_OK;
  }else if(0==strcmp("show jobs", actcmd)) {     // show jobs
    int n_puts = Tcl_WriteChars(sock,"show jobs\n",-1);
    Tcl_Obj *list = Tcl_NewListObj(0, NULL);
    while(1){
      Tcl_Obj *lineObj = Tcl_NewObj();
      int size = Tcl_GetsObj(sock, lineObj);

      if(0==strcmp(".",Tcl_GetString(lineObj))){
        break;
      }
      Tcl_ListObjAppendElement(interp, list, lineObj);
      debug_info("%s", Tcl_GetString(lineObj));
    }
    Tcl_SetObjResult(interp,list);
    return TCL_OK;
  }else if(0==strcmp("drop function", actcmd)) {     // drop function $function
    return TCL_OK;
  }else if(0==strcmp("create function", actcmd)) {     // create function $function
    return TCL_OK;
  }else if(0==strcmp("getpid", actcmd)) {     // getpid
    Tcl_Obj *result = read_sock_reply(interp, sock, actcmd);
    Tcl_SetObjResult(interp, result);
    return TCL_OK;
  }else{
    log_error("Unsupported subcmd %s", actcmd);
    return TCL_ERROR;
  }

  return TCL_OK;
}
/////// Register Tcl /////////
extern "C" {

int Tclgearman_Init(Tcl_Interp *interp) {

#ifdef USE_TCL_STUBS
 if(Tcl_InitStubs(interp, "8.4", 0) == NULL) {
   debug_error("Tcl_InitStubs");
   return TCL_ERROR;
 }
#else
 if(Tcl_PkgRequire(interp, "Tcl", "8.4", 0) == NULL) {
   debug_error("package require Tcl 8.4");
   return TCL_ERROR;
 }
#endif
 Tcl_PkgProvide(interp, "gearman", "0.01");

 Tcl_CreateObjCommand(interp, "gearman::client", TclObjCmd_gearman_client, &client, NULL);
 Tcl_CreateObjCommand(interp, "gearman::worker", TclObjCmd_gearman_worker, &client, NULL);
 Tcl_CreateObjCommand(interp, "gearman::admin",  TclObjCmd_gearman_admin, NULL, NULL);

 debug_info("GearmanTcl loaded!");
 return TCL_OK;
}

}


// TOOD: -timeout

