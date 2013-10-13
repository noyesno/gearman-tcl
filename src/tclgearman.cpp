/**************************************************************
* Gearman Tcl Client/Worker/Admin
* -------------------------------------------------------------
* by: Sean Zhang
: at: Oct, 2013
***************************************************************/

#include "../generic/dbg.h"
#include <libgearman/gearman.h>

#include <tcl.h>
#include <stdio.h>
#include <iostream>


// TODO: allow multiple instance!!!
extern "C" {
  gearman_client_st client;
  gearman_worker_st *worker;
}




int TclObjCmd_gearman_client_create(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *const objv[]) {

  gearman_client_st &client = *(gearman_client_st *)clientData;

  if (gearman_client_create(&client) == NULL){
    debug_error("Memory allocation failure on client creation");
    return TCL_ERROR;
  }

  const char *host = Tcl_GetString(objv[2]);
  in_port_t port = GEARMAN_DEFAULT_TCP_PORT;

  gearman_return_t ret;
  ret= gearman_client_add_server(&client, host, port);
  if (ret != GEARMAN_SUCCESS) {
    log_error("Gearman add server FAIL: %s:%d", host, port);
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
    if(0==strcmp("-client_id",name)){
      int size = 0;
      const char *value = Tcl_GetStringFromObj(objv[j], &size);
      debug_info("client config -client_id %s", value);
      if (gearman_failed(gearman_client_set_identifier(&client, value, size))){
        std::cerr << gearman_worker_error(worker) << std::endl;
        return TCL_ERROR;
      }
      return TCL_OK;
    }else if(0==strcmp("-timeout",name)){
      int timeout = 0;
      Tcl_GetIntFromObj(interp, objv[4], &timeout);
      gearman_client_set_timeout(&client, timeout);
      return TCL_OK;
    }else{
      debug_info("Unsupported client config %s", name);
      return TCL_ERROR;
    }
  }
  return TCL_OK;
}

int TclObjCmd_gearman_client_submit(ClientData clientData, Tcl_Interp *interp, int objc, Tcl_Obj *const objv[]) {
  gearman_client_st &client = *(gearman_client_st *)clientData;
  gearman_return_t ret;

  const char *command = Tcl_GetString(objv[2]);
  const char *data    = Tcl_GetString(objv[3]);
  debug("%s < %s ;# %s %s", command, data, Tcl_GetString(objv[0]), Tcl_GetString(objv[1]));

  size_t result_size;
  char *result;

  result= (char *)gearman_client_do(&client, command, NULL,
                                      data, strlen(data), // TODO
                                      &result_size, &ret);
  if (ret == GEARMAN_WORK_DATA) {
      debug("GEARMAN_WORK_DATA");
      std::cout.write(result, result_size);
      free(result);
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
      free(result);

      return TCL_OK;
  } else if (ret == GEARMAN_WORK_FAIL) {
      debug_error("GEARMAN_WORK_FAIL");
      return TCL_ERROR;
  } else if (ret == GEARMAN_COULD_NOT_CONNECT) {
      debug_error("Can not connect to server %s", gearman_client_error(&client));
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
    return TclObjCmd_gearman_client_submit(clientData, interp, objc-1, objv+1);
  }else if(0==strcmp("config", actcmd)) {
    return TclObjCmd_gearman_client_config(clientData, interp, objc, objv);
  }else if(0==strcmp("close", actcmd)) {
    gearman_client_free(&client);
    return TCL_OK;
  } else {
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
  if (gearman_failed(gearman_worker_add_server(worker, host, port))) {
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
    if(0==strcmp("-client_id",name)){
      int size = 0;
      const char *value = Tcl_GetStringFromObj(objv[j], &size);
      if (gearman_failed(gearman_worker_set_identifier(worker, value, size))){
        std::cerr << gearman_worker_error(worker) << std::endl;
        return TCL_ERROR;
      }
      return TCL_OK;
    }else if(0==strcmp("-timeout",name)){
      int timeout = 0;
      Tcl_GetIntFromObj(interp, objv[4], &timeout);
      gearman_worker_set_timeout(worker, timeout);
      return TCL_OK;
    }
  }
  return TCL_OK;
}

int Tcl_gearman_worker_register_task(gearman_worker_st *worker,
   const char *task_name, const char *task_proc=NULL)
{
  // register function
  gearman_function_t worker_fn= gearman_function_create(gearman_tcl_worker);

  if (gearman_failed(gearman_worker_define_function(worker,
                                                    task_name, strlen(task_name),
                                                    worker_fn,
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


int Tcl_gearman_worker_work(gearman_worker_st *worker){
  // work infinit
  while(1){
    debug_info("work");
    if (gearman_failed(gearman_worker_work(worker))){
      debug_error("work fail", gearman_worker_error(worker));
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
    return Tcl_gearman_worker_register_task(worker, task_name, NULL);
  }else if(0==strcmp("work", actcmd)) {
    tcl_worker_context.worker = objv[1];
    tcl_worker_context.interp = interp;
    return Tcl_gearman_worker_work(worker);
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

 Tcl_CreateObjCommand(interp, "gearman::client", TclObjCmd_gearman_client, &client, NULL);
 Tcl_CreateObjCommand(interp, "gearman::worker", TclObjCmd_gearman_worker, &client, NULL);

 debug_info("GearmanTcl loaded!");
 return TCL_OK;
}

}
