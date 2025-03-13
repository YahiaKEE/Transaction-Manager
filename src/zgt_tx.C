      /***************** Transaction class **********************/
      /*** Implements methods that handle Begin, Read, Write, ***/
      /*** Abort, Commit operations of transactions. These    ***/
      /*** methods are passed as parameters to threads        ***/
      /*** spawned by Transaction manager class.              ***/
      /**********************************************************/

      /* Spring 2025: CSE 4331/5331 Project 2 : Tx Manager */

      //3/13/25 12:21 AM

      /* Required header files */
      #include <stdio.h>
      #include <stdlib.h>
      #include <sys/signal.h>
      #include "zgt_def.h"
      #include "zgt_tm.h"
      #include "zgt_extern.h"
      #include <unistd.h>
      #include <iostream>
      #include <fstream>
      #include <pthread.h>


      extern void *start_operation(long, long);  //starts opeartion by doing conditional wait
      extern void *finish_operation(long);       // finishes abn operation by removing conditional wait
      extern void *do_commit_abort_operation(long, char);   //commit/abort based on char value 
      //extern void *process_read_write_operation(long, long, int, char); //did not use

      extern zgt_tm *ZGT_Sh;			// Transaction manager object
      FILE *logfile; //Global variable

      /* Transaction class constructor */
      /* Initializes transaction id and status and thread id */
      /* Input: Transaction id, status, thread id */

      zgt_tx::zgt_tx( long tid, char Txstatus,char type, pthread_t thrid){
        this->lockmode = (char)' ';   // default
        this->Txtype = type;          // R = read only, W=Read/Write
        this->sgno =1;
        this->tid = tid;
        this->obno = -1;              // set it to a invalid value
        this->status = Txstatus;
        this->pid = thrid;
        this->head = NULL;
        this->nextr = NULL;
        this->semno = -1;             // init to an invalid sem value

        // Open logfile if not already opened
       if (logfile == NULL) 
       {
      logfile = fopen(ZGT_Sh->logfilename, "a");
      if (logfile == NULL) {
          printf("\nERROR: Cannot open log file for append: %s\n", ZGT_Sh->logfilename);
          fflush(stdout);
          exit(1);
      }
       }
      }

      /* Method used to obtain reference to a transaction node      */
      /* Inputs the transaction id. Makes a linear scan over the    */
      /* linked list of transaction nodes and returns the reference */
      /* of the required node if found. Otherwise returns NULL      */

      zgt_tx* get_tx(long tid1){
        zgt_tx *txptr, *lastr1;

        if(ZGT_Sh->lastr != NULL){	// If the list is not empty
            lastr1 = ZGT_Sh->lastr;	// Initialize lastr1 to first node's ptr
            for  (txptr = lastr1; (txptr != NULL); txptr = txptr->nextr)
      	    if (txptr->tid == tid1) 		// if required id is found
      	       return txptr;
            return (NULL);			// if not found in list return NULL
         }
        return(NULL);				// if list is empty return NULL
      }

      /* Method that handles "BeginTx tid" in test file     */
      /* Inputs a pointer to transaction id, obj pair as a struct. Creates a new  */
      /* transaction node, initializes its data members and */
      /* adds it to transaction list */

      void *begintx(void *arg){ 

        //initialise a transaction object. Make sure it is
        //done after acquiring the semaphore for the tm and making sure that
        //the operation can proceed using the condition variable. when creating
        //the tx object, set the tx to TR_ACTIVE and obno to -1; there is no
        //semno as yet as none is waiting on this tx.

        struct param *node = (struct param*)arg; // get tid and count
    start_operation(node->tid, node->count);

    zgt_tx *tx = new zgt_tx(node->tid, TR_ACTIVE, node->Txtype, pthread_self()); // Create new tx node

        // Writes the Txtype to the file.

    zgt_p(0);  // Lock Tx manager; Add node to transaction list
    fprintf(ZGT_Sh->logfile, "T%d\t%c \tBeginTx\n", node->tid, node->Txtype);
    fflush(ZGT_Sh->logfile);
    tx->nextr = ZGT_Sh->lastr;
    ZGT_Sh->lastr = tx;
    zgt_v(0); 			// Release tx manager 
    finish_operation(node->tid);
    pthread_exit(NULL);				// thread exit
      }

      /* Method to handle Readtx action in test file    */
      /* Inputs a pointer to structure that contans     */
      /* tx id and object no to read. Reads the object  */
      /* if the object is not yet present in hash table */
      /* or same tx holds a lock on it. Otherwise waits */
      /* until the lock is released */


      void *readtx(void *arg) {
        struct param *node = (struct param*)arg; // Extract transaction parameters
    
        // Start transaction operation, ensuring proper execution order
        start_operation(node->tid, node->count);
        zgt_p(0); // Lock transaction manager to prevent race conditions
    
        // Retrieve the transaction reference
        zgt_tx *activeTx = get_tx(node->tid);
    
        // If the transaction is invalid or has been aborted, log an error and exit
        if (!activeTx || activeTx->status == 'A') {
            fprintf(logfile, "[ERROR] Transaction %ld not found or aborted. Read operation failed.\n", node->tid);
            fflush(logfile);
            zgt_v(0); // Release transaction manager lock
            finish_operation(node->tid);
            pthread_exit(NULL);
        }
    
        // Ensure the transaction is active before proceeding
        if (activeTx->status != TR_ACTIVE) {
            fprintf(logfile, "[WARNING] Transaction %ld is not active. Read operation skipped.\n", node->tid);
            fflush(logfile);
            zgt_v(0);
            finish_operation(node->tid);
            pthread_exit(NULL);
        }
    
        // Release transaction manager lock before attempting lock acquisition
        zgt_v(0);
    
        // Attempt to acquire a shared lock for reading
        bool lockAcquired = (activeTx->set_lock(node->tid, activeTx->sgno, node->obno, node->count, 'S') == 1);
    
        if (lockAcquired) {
            // Perform the read operation only if the lock was successfully acquired
            activeTx->perform_read_write_operation(activeTx->tid, node->obno, 'S');
        } else {
            // Log failure to acquire lock
            fprintf(logfile, "[WARNING] Transaction %ld failed to acquire a shared lock on object %ld.\n", node->tid, node->obno);
            fflush(logfile);
        }
    
        // Finalize the transaction operation and terminate the thread
        finish_operation(node->tid);
        pthread_exit(NULL);
    }
    
    
    

    void *writetx(void *arg) {
      struct param *node = (struct param*)arg; // Extract transaction parameters
  
      // Begin transaction execution and acquire lock for controlled access
      start_operation(node->tid, node->count);
      zgt_p(0); // Lock transaction manager to ensure safe concurrent access
  
      // Fetch transaction details
      zgt_tx *activeTransaction = get_tx(node->tid);
  
      // If transaction is valid and not aborted, proceed
      if (!activeTransaction || activeTransaction->status == 'A') {
          fprintf(logfile, "ERROR: Transaction %ld does not exist or has been aborted. Skipping write operation.\n", node->tid);
          fflush(logfile);
          zgt_v(0); // Release transaction manager lock before exiting
          finish_operation(node->tid);
          pthread_exit(NULL);
      }
  
      // Retrieve object from hash table
      zgt_hlink *objectRecord = ZGT_Ht->find(activeTransaction->sgno, node->obno);
  
      // Validate that the transaction is still active before proceeding
      if (activeTransaction->status != TR_ACTIVE) {
          zgt_v(0); // Unlock before exiting
          finish_operation(node->tid);
          pthread_exit(NULL);
      }
  
      // Unlock transaction manager before acquiring object lock
      zgt_v(0);
  
      // Attempt to obtain an exclusive lock for writing
      bool writeLockGranted = (activeTransaction->set_lock(
          node->tid, activeTransaction->sgno, node->obno, node->count, 'X') == 1
      );
  
      // Perform the write operation if lock acquisition was successful
      if (writeLockGranted) {
          activeTransaction->perform_read_write_operation(activeTransaction->tid, node->obno, 'X');
      } else {
          fprintf(logfile, "WARNING: Transaction %ld could not obtain a lock on object %ld.\n", node->tid, node->obno);
          fflush(logfile);
      }
  
      // Complete transaction execution and terminate the thread
      finish_operation(node->tid);
      pthread_exit(NULL);
  }

  void *aborttx(void *arg) {
    struct param *node = (struct param*)arg; // Extract transaction parameters

    // Begin operation sequence, ensuring thread synchronization
    start_operation(node->tid, node->count);
    zgt_p(0); // Lock the transaction manager to ensure consistency

    // Retrieve the transaction information
    zgt_tx *txn = get_tx(node->tid);

    // Ensure the transaction exists before proceeding
    if (!txn) {
        fprintf(logfile, "ERROR: Transaction %ld not found. Abort operation canceled.\n", node->tid);
        fflush(logfile);
        zgt_v(0); // Release lock to prevent deadlock
        finish_operation(node->tid);
        pthread_exit(NULL);
    }

    // Perform abort operation and remove the transaction from the system
    do_commit_abort_operation(node->tid, 'A');

    // Ensure locks are released if the transaction held any
    txn->free_locks();

    // Remove the transaction node from the transaction manager list
    txn->remove_tx();

    // Unlock the transaction manager before finishing
    zgt_v(0);

    // Log successful transaction abort
    fprintf(logfile, "Transaction %ld aborted successfully.\n", node->tid);
    fflush(logfile);

    // Complete transaction cleanup and terminate thread
    finish_operation(node->tid);
    pthread_exit(NULL);
}


void *committx(void *arg) {
  struct param *node = (struct param*)arg; // Extract transaction details

  // Begin execution and ensure proper synchronization
  start_operation(node->tid, node->count);
  zgt_p(0); // Lock transaction manager to maintain consistency

  // Locate the transaction in the system
  zgt_tx *transaction = get_tx(node->tid);

  // Check if transaction exists before proceeding
  if (transaction == NULL) {
      fprintf(logfile, "ERROR: Transaction %ld not found. Commit failed.\n", node->tid);
      fflush(logfile);
      zgt_v(0); // Unlock transaction manager before exit
      finish_operation(node->tid);
      pthread_exit(NULL);
  }

  // Update transaction status to indicate it's being committed
  transaction->status = 'E';

  // Debug print to track commit execution
  printf("[Commit] Transaction: %ld | Semaphore ID: %d\n", node->tid, transaction->semno);

  // Process commit operation
  do_commit_abort_operation(node->tid, 'E');

  // Release any locks held by this transaction before removing it
  transaction->free_locks();

  // Remove the transaction from the system
  transaction->remove_tx();

  // Unlock transaction manager after the commit process is complete
  zgt_v(0);

  // Log successful commit operation
  fprintf(logfile, "Transaction %ld committed successfully.\n", node->tid);
  fflush(logfile);

  // Finalize execution and terminate thread
  finish_operation(node->tid);
  pthread_exit(NULL);
}


      //suggestion as they are very similar

      // called from commit/abort with appropriate parameter to do the actual
      // operation. Make sure you give error messages if you are trying to
      // commit/abort a non-existent tx


      void *do_commit_abort_operation(long transactionID, char actionType) {
        zgt_tx *transactionRef = get_tx(transactionID);
    
        if (!transactionRef) {
            fprintf(logfile, "[ERROR] Transaction %ld not found. %s operation failed.\n",
                    transactionID, (actionType == 'A') ? "Abort" : "Commit");
            fflush(logfile);
            return NULL;
        }
    
        // Unlock resources before modifying state
        transactionRef->free_locks();
    
        int semaphoreRef = transactionRef->semno;
    
        if (semaphoreRef > -1) {
            int waitingCount = zgt_nwait(semaphoreRef);
            for (int i = 0; i < waitingCount; ++i) {
                zgt_v(semaphoreRef);
            }
        }
    
        if (transactionRef->end_tx() != 0) {
            fprintf(logfile, "[ERROR] Transaction %ld failed to terminate cleanly.\n", transactionID);
            fflush(logfile);
            return NULL;
        }
    
        fprintf(logfile, "[LOG] Transaction %ld %s successfully.\n",
                transactionID, (actionType == 'A') ? "Aborted" : "Committed");
        fflush(logfile);
    
        return NULL;
    }    
    
      int zgt_tx::remove_tx ()
      {
        //remove the transaction from the TM

        zgt_tx *txptr, *lastr1;
        lastr1 = ZGT_Sh->lastr;
        for(txptr = ZGT_Sh->lastr; txptr != NULL; txptr = txptr->nextr){	// scan through list
      	  if (txptr->tid == this->tid){		// if correct node is found  
      		 lastr1->nextr = txptr->nextr;	// update nextr value; done
      		 //delete this;
               return(0);
      	  }
      	  else lastr1 = txptr->nextr;			// else update prev value
         }

         //check two below lines --------
        fprintf(ZGT_Sh->logfile, "Trying to Remove a Tx:%d that does not exist\n", this->tid);
        fflush(ZGT_Sh->logfile);
        printf("Trying to Remove a Tx:%d that does not exist\n", this->tid);
        fflush(stdout);
        return(-1);
      }

      /* this method sets lock on objno1 with lockmode1 for a tx in this*/

      int zgt_tx::set_lock(long txnID, long segmentID, long objectID, int sequence, char lockType) {
        bool lockAcquired = false;
    
        while (!lockAcquired) {
            // Secure transaction manager for safe lock modification
            zgt_p(0);
            zgt_tx *currentTransaction = get_tx(txnID);
    
            // Validate transaction existence before proceeding
            if (!currentTransaction) {
                fprintf(logfile, "[ERROR] Transaction %ld not found. Lock acquisition failed.\n", txnID);
                fflush(logfile);
                zgt_v(0);
                return -1;  // Exit with failure
            }
    
            // Locate object in hash table
            zgt_hlink *objectReference = ZGT_Ht->find(currentTransaction->sgno, objectID);
    
            if (!objectReference) {
                // Grant new lock since object is not held by anyone
                ZGT_Ht->add(currentTransaction, currentTransaction->sgno, objectID, currentTransaction->lockmode);
                currentTransaction->status = 'P';  
                zgt_v(0);
                return 1;
            }
    
            // Check if transaction already holds the lock
            if (objectReference->tid == txnID) {
                currentTransaction->status = 'P';  
                zgt_v(0);
                return 1;
            }
    
            // Determine the status of the transaction currently holding the object
            zgt_tx *existingTransaction = get_tx(objectReference->tid);
            zgt_hlink *pendingTransaction = others_lock(objectReference, segmentID, objectID);
    
            // Shared read access case
            if (currentTransaction->Txtype == 'R' && existingTransaction->Txtype == 'R' && pendingTransaction->lockmode != 'X') {
                lockAcquired = true;
    
                // Assign lock and update linked list properly
                if (!currentTransaction->head) {
                    ZGT_Ht->add(currentTransaction, currentTransaction->sgno, objectID, currentTransaction->lockmode);
                    objectReference = ZGT_Ht->find(currentTransaction->sgno, objectID);
                    currentTransaction->head = objectReference;
                } else {
                    zgt_hlink *iterator = currentTransaction->head;
                    while (iterator->nextp) {
                        iterator = iterator->nextp;
                    }
                    iterator->nextp = objectReference;
                }
    
                currentTransaction->status = 'P';
                zgt_v(0);
                return 1;
            } else {
                // Transaction must wait
                currentTransaction->status = 'W';  
                currentTransaction->obno = objectID;
                currentTransaction->lockmode = lockType;
    
                if (get_tx(objectReference->tid)) {
                    currentTransaction->setTx_semno(objectReference->tid, objectReference->tid);
                } else {
                    currentTransaction->status = 'P';
                    zgt_v(0);
                    return 1;
                }
    
                fprintf(logfile, "[INFO] Transaction %ld is waiting on Transaction %ld for object %ld.\n",
                        currentTransaction->tid, objectReference->tid, objectID);
                fflush(logfile);
    
                zgt_v(0);
                zgt_p(objectReference->tid);
                lockAcquired = false;
            }
        }
    }
    
    
    
    

      int zgt_tx::free_locks()
      {

        // this part frees all locks owned by the transaction
        // that is, remove the objects from the hash table
        // and release all Tx's waiting on this Tx

        zgt_hlink* temp = head;  //first obj of tx

        for(temp;temp != NULL;temp = temp->nextp){	// SCAN Tx obj list

            //check two below lines compare to skeleton code
            fprintf(logfile, "%d : %d \t", temp->obno, ZGT_Sh->objarray[temp->obno]->value);
            fflush(logfile);

            if (ZGT_Ht->remove(this,1,(long)temp->obno) == 1){
          	   printf(":::ERROR:node with tid:%d and onjno:%d was not found for deleting", this->tid, temp->obno);		// Release from hash table
          	   fflush(stdout);
            }
            else {
      #ifdef TX_DEBUG
      	   printf("\n:::Hash node with Tid:%d, obno:%d lockmode:%c removed\n",
                                  temp->tid, temp->obno, temp->lockmode);
      	   fflush(stdout);
      #endif
            }
          }

          //check again below 2 lines compare w skeleton
        fprintf(logfile, "\n");
        fflush(logfile);

        return(0);
      }

      // CURRENTLY Not USED
      // USED to COMMIT
      // remove the transaction and free all associate dobjects. For the time being
      // this can be used for commit of the transaction.

      int zgt_tx::end_tx()  
      {
        zgt_tx *linktx, *prevp;

        // USED to COMMIT 
        //remove the transaction and free all associate dobjects. For the time being 
        //this can be used for commit of the transaction.

        linktx = prevp = ZGT_Sh->lastr;

        while (linktx){
          if (linktx->tid  == this->tid) break;
          prevp  = linktx;
          linktx = linktx->nextr;
        }
        if (linktx == NULL) {
          printf("\ncannot remove a Tx node; error\n");
          fflush(stdout);
          return (1);
        }
        if (linktx == ZGT_Sh->lastr) ZGT_Sh->lastr = linktx->nextr;
        else {
          prevp = ZGT_Sh->lastr;
          while (prevp->nextr != linktx) prevp = prevp->nextr;
          prevp->nextr = linktx->nextr;
        }
      }

      // currently not used
      int zgt_tx::cleanup()
      {
        return(0);

      }

      //below is NOT in skeleton code
      // check which other transaction has the lock on the same obno
      // returns the hash node
      zgt_hlink *zgt_tx::others_lock(zgt_hlink *hnodep, long sgno1, long obno1)
      {
        zgt_hlink *ep;
        ep=ZGT_Ht->find(sgno1,obno1);
        while (ep)				// while ep is not null
          {
            if ((ep->obno == obno1)&&(ep->sgno ==sgno1)&&(ep->tid !=this->tid))
      	return (ep);			// return the hashnode that holds the lock
            else  ep = ep->next;
          }
        return (NULL);			//  Return null otherwise

      }

      // routine to print the tx list
      // TX_DEBUG should be defined in the Makefile to print

      void zgt_tx::print_tm(){

        zgt_tx *txptr;

      #ifdef TX_DEBUG
        printf("printing the tx list \n");
        printf("Tid\tTxType\tThrid\t\tobjno\tlock\tstatus\tsemno\n");
        fflush(stdout);
      #endif
        txptr=ZGT_Sh->lastr;
        while (txptr != NULL) {
      #ifdef TX_DEBUG
          printf("%d\t%c\t%d\t%d\t%c\t%c\t%d\n", txptr->tid, txptr->Txtype, txptr->pid, txptr->obno, txptr->lockmode, txptr->status, txptr->semno);
          fflush(stdout);
      #endif
          txptr = txptr->nextr;
        }
        fflush(stdout);
      }

      //currently not used
      void zgt_tx::print_wait(){

        //route for printing for debugging

        printf("\n    SGNO        TxType       OBNO        TID        PID         SEMNO   L\n");
        printf("\n");
      }
      void zgt_tx::print_lock(){
        //routine for printing for debugging

        printf("\n    SGNO        OBNO        TID        PID   L\n");
        printf("\n");

      }

      // routine to perform the acutual read/write operation
      // based  on the lockmode
      void zgt_tx::perform_read_write_operation(long txnID, long objectID, char mode) {
        zgt_p(0);
    
        zgt_tx *activeTxn = get_tx(txnID);
    
        if (!activeTxn) {
            fprintf(logfile, "[ERROR] Transaction %ld not found! Read/Write operation aborted.\n", txnID);
            fflush(logfile);
            zgt_v(0);
            return;
        }
    
        if (!ZGT_Sh->objarray[objectID]) {
            fprintf(logfile, "[ERROR] Object %ld does not exist! Transaction %ld cannot proceed.\n", objectID, txnID);
            fflush(logfile);
            zgt_v(0);
            return;
        }
    
        int previousValue = ZGT_Sh->objarray[objectID]->value;
    
        if (activeTxn->status != TR_ACTIVE) {
            fprintf(logfile, "[WARNING] Transaction %ld is not active! Skipping operation.\n", txnID);
            fflush(logfile);
            zgt_v(0);
            return;
        }
    
        switch (mode) {
            case 'S':
                ZGT_Sh->objarray[objectID]->value--;
                fprintf(logfile, "T%ld\t  \tReadTx \t\t %ld:%d:%d  \t\t ReadLock \t Granted \t%c\n",
                        txnID, objectID, previousValue - 1, ZGT_Sh->optime[txnID], activeTxn->status);
                fflush(logfile);
                break;
    
            case 'X':
                ZGT_Sh->objarray[objectID]->value++;
                fprintf(logfile, "T%ld\t  \tWriteTx \t %ld:%d:%d  \t\t WriteLock \t Granted \t%c\n",
                        txnID, objectID, previousValue + 1, ZGT_Sh->optime[txnID], activeTxn->status);
                fflush(logfile);
                break;
    
            default:
                fprintf(stderr, "[ERROR] Invalid lock mode '%c' for transaction %ld on object %ld.\n", mode, txnID, objectID);
                fprintf(logfile, "[ERROR] Invalid lock mode '%c' for transaction %ld on object %ld.\n", mode, txnID, objectID);
                fflush(stderr);
                fflush(logfile);
                break;
        }
    
        zgt_v(0);
    }
    
    
    


      // routine that sets the semno in the Tx when another tx waits on it.
      // the same number is the same as the tx number on which a Tx is waiting
      int zgt_tx::setTx_semno(long tid, int semno){
        zgt_tx *txptr;

        txptr = get_tx(tid);
        if (txptr == NULL){
          printf("\n:::ERROR:Txid %d wants to wait on sem:%d of tid:%d which does not exist\n", this->tid, semno, tid);
          fflush(stdout);
          return(-1);
        }
        if ((txptr->semno == -1)|| (txptr->semno == semno)){  //just to be safe
          txptr->semno = semno;
          return(0);
        }
        else if (txptr->semno != semno){
      #ifdef TX_DEBUG
          printf(":::ERROR Trying to wait on sem:%d, but on Tx:%d\n", semno, txptr->tid);
          fflush(stdout);
      #endif
          exit(1);
        }
        return(0);
      }

      

      void *start_operation(long tid, long count){

        pthread_mutex_lock(&ZGT_Sh->mutexpool[tid]);	// Lock mutex[t] to make other
        // threads of same transaction to wait

        while(ZGT_Sh->condset[tid] != count)		// wait if condset[t] is != count
          pthread_cond_wait(&ZGT_Sh->condpool[tid],&ZGT_Sh->mutexpool[tid]);

      }

      // Otherside of the start operation;
      // signals the conditional broadcast

      void *finish_operation(long tid){
        ZGT_Sh->condset[tid]--;	// decr condset[tid] for allowing the next op
        pthread_cond_broadcast(&ZGT_Sh->condpool[tid]);// other waiting threads of same tx
        pthread_mutex_unlock(&ZGT_Sh->mutexpool[tid]);
      }
