import sys;
ts =0
transaction_table={}
lock_table={}

def read_item(line):
	
	status_RL = "read_lock"
	executing_transactions = transaction_table[line[1]]#Current Transaction id is extracted from the transaction table
	
	
	item_id = line[3]
	transaction_id = line[1]

	if executing_transactions['transaction_phase'] == True:# Phaseof the transaction is true when it is in growing phase(Wherein read, write , upgrade is done and the data items can only acquire the locks and none cane be released or downgraded)
	
		if item_id in lock_table: # Check if there is entry of item id in the lock table
			executing_lock = lock_table[item_id]# Instantiating the current lock to the corresponding item id in the lock table
			if executing_lock['lock_status'] == "read_lock":
				if line[1] not in executing_lock['locking_transactions']:  #if this trasaction is not available in locking transaction make an entry in the locking transactions list against the item id
					(executing_lock['locking_transactions']).append(line[1])
					executing_transactions['transaction_item'].append(item_id)
					print("\nExecuting " + line + "\nChanges in Tables: The item " + str(item_id)+ "'s lock status for the transaction "  + str(transaction_id) + " is set to Read lock in the lock table")

			# this block is for downgrading from write lock to read lock, should be in phase = false
			elif executing_lock['lock_status'] == "write_lock":
				if len(executing_lock['locking_transactions'])==0: # Before upgrading an item lock status from read lock to write lock we have to check if there are multiple read locks on the particular item
					executing_lock['lock_status'] = status_RL
					if item_id not in executing_transactions['transaction_item']:
						executing_transactions['transaction_item'].append(item_id)

					print("\nExecuting " + line + "\nChanges in Tables: The item " + str(item_id) + "'s lock status for the transaction " + str(transaction_id) + " is downgraded from 'Write lock' to 'Read lock' in the lock table")

				elif line[1] not in executing_lock['locking_transactions']:
					t = executing_lock['locking_transactions'][0]# If the item is not WL'ed by that transaction then check for the priority of transaction among the conflicting transactions based on wound wait protocol
					woundwait(t,line[1],line[0],item_id,line,1)
			
			elif executing_lock['lock_status'] ==' ':
				if line[1] not in executing_lock['locking_transactions']:  #if this trasactionis not available in locking transaction make an entry in the locking transactions list against the item id
					(executing_lock['locking_transactions']).append(line[1])
					executing_transactions['transaction_item'].append(item_id)
					executing_lock['lock_status']=status_RL
					print("\nExecuting " + line + "\nChanges in Tables: The item " + str(item_id)+ "'s lock status for the transaction "  + str(transaction_id) + " is set to Read lock in the lock table")

			
		else:	# if the item is not available in the lock table, then make new entry in the lock table
			lock_table[item_id] = {'lock_status': status_RL, 'locking_transactions': [line[1]], 'waiting_transactions': []}
			transaction_table[line[1]]['transaction_item'].append(item_id)
			print("\nExecuting " + line + "\nChanges in Tables: Added item_id entry into lock table, Lock Status is set to 'Read Lock', The item " + str(item_id) + " for transaction " + str(transaction_id) +" is newly added into lock table and also added the item to transaction table in corresponding transaction")

		

def write_item(line):

	operation = line[0]	
	transaction_id = line[1]
	item_id = line[3]
	executing_transactions = transaction_table[transaction_id]

	if transaction_table[transaction_id]['transaction_phase'] == True:# Phaseof the transaction is true when it is in growing phase(Wherein read, write , upgrade is done and the dataitems can only acquire the locks and none cane be released or downgraded)
		
		if item_id in lock_table:# Check if there is entry of item id in the lock table
			executing_lock = lock_table[item_id]# Instantiating the current lock to the corresponding item id in the lock table
			t_list = executing_lock['locking_transactions']# retrieving the list of locking transactions of the current item
			if executing_lock['lock_status'] == "read_lock":
				if (len(t_list)==1 and t_list[0] == transaction_id):#Only one transaction can hold this item with read lock inorder to directly upgrade to the write lock
					executing_lock['lock_status'] = "write_lock"
					print("\nExecuting " + line + "\nChanges in Tables: The item " + str(item_id) + "'s lock status for transaction "+ str(transaction_id) +" is upgraded from 'Read Lock' to 'Write Lock'")

				else: # IF the list has more than one transaction holding the read lock on current item check which transaction should be executed first
					if executing_transactions['transaction_state'] == "blocked":
						executing_transactions['waiting_operations'].append({'operation':line, 'item_id':item_id})
						print("\nExecuting " + line + "\nTransaction " +str(transaction_id)+ "is already blocked, so this operation is will resume when the transaction " + str(transaction_id) + " unblocks.")

					if executing_transactions['transaction_state'] == "aborted":
						print("\nExecuting " + line + "\nTransaction " +str(transaction_id)+ "is already aborted, so this operation is disregarded")

					print("\nExecuting " + line + "\nChanges in Tables: Found multiple read locks on the item " + str(item_id) + " already. The write item operation for this transaction " + str(transaction_id) + ". and the read locks by other transaction(s) are conflicting operations. The wound-wait protocol is activated to resolve the situation.")
					
					temp=0
					for t1 in executing_lock['locking_transactions']:
						temp+=1
						woundwait(t1,transaction_id,operation,item_id,line,temp)#check the timestamp of the current transaction against all the transactions in the list so as to abort or terminate one among them using wound-wait protocol
						
							

			elif executing_lock['lock_status'] == "write_lock":
				if transaction_id not in executing_lock['locking_transactions']:
					t = executing_lock['locking_transactions'][0] # Since it already has write lock on the item the wound-wait algorithm has to be performed to know which transaction should be granted write lock
					woundwait(t,transaction_id,operation,item_id,line,1)
				
			elif executing_lock['lock_status'] == " ":
				
				executing_lock['lock_status'] = "write_lock"
				if line[1] not in executing_lock['locking_transactions']:
					(executing_lock['locking_transactions']).append(line[1])
				executing_transactions['transaction_item'].append(item_id)
				print("\nExecuting " + line + "\nChanges in Tables: The item " + str(item_id) + "'s lock status for transaction "+ str(transaction_id) +" is set to 'Write Lock'")
				
				

		else: # if the item is not available in the lock table, then make new entry in the lock table
			lock_table[item_id] = {'lock_status': "write_lock", 'locking_transactions': [transaction_id], 'waiting_transactions': []}
			transaction_table[transaction_id]['transaction_item'].append(item_id)
			print("\nExecuting " + line + "\nChanges in Tables: Added item_id entry into lock table, Lock Status is set to 'Write Lock', The item " + str(item_id) + " for transaction " + str(transaction_id) +" is newly added into lock table and also added the item to transaction table in corresponding transaction")



def woundwait(t1,t2,operation,item_id,line,temp):
	
	executing_lock = lock_table[item_id]
	t_list = lock_table[item_id]['locking_transactions']
	Trans1 = transaction_table[t1]
	Trans2= transaction_table[t2]
	TS1 = Trans1['Timestamp']
	TS2 = Trans2['Timestamp']

			
	if Trans2['transaction_state'] != "blocked" or Trans2['transaction_state'] != "aborted": # the conflicting transaction should also be active in order to implement wait die protocol
		
		if TS2 < TS1: # If TS2 is older than TS1, Trans2 is made to wait and Trans1 is aborted
			if temp == 1:
				Trans2['waiting_operations'].append({'operation':line, 'item_id':item_id})
				executing_lock['waiting_transactions'].append(t2) # The TS2 is being added to the list of waiting transaction for the particular item
				Trans1['transaction_state'] = "aborted"
				print("Changes in Tables: Transaction "+ str(t1) + " is aborted. Also, transaction" + str(t2) + "has been provided with the item" + str(item_id) + " based on wound wait protocol.")
				end_transaction(t1,"aborted",line)
			

		  
		else:
			if temp ==1:
				print((Trans2['waiting_operations']))
				Trans2['waiting_operations'].append({'operation':line, 'item_id':item_id})
				executing_lock['waiting_transactions'].append(t2) # The TS2 is being added to the list of waiting transaction for the particular item
				print("\nExecuting " + line + "\nChanges in Tables: Transaction "+ str(t2) +" is blocked, and the operation " + operation +  "is added to list of waiting operations : " ,Trans2['waiting_operations'], "\nAlso, for the item " + item_id + " the transaction is added to the list of waiting transactions in the lock table : ",executing_lock['waiting_transactions'], " based on wound wait protocol.")
				
			
			
def end_transaction(t,t_state,line):
	try:
		
		executing_transactions = transaction_table[t]
		executing_transactions['transaction_state']=t_state
		executing_transactions['transaction_phase'] = False # Phase of the current transaction is set to false since it goes to shrinking phase(Wherein unlock and downgrading is done)

		for d_item in executing_transactions['transaction_item']:
		
			executing_lock = lock_table[d_item]
			t_list = executing_lock['locking_transactions']
			waiting_t_list = executing_lock['waiting_transactions']
			
			executing_lock['lock_status'] = " "
			
			for i in t_list:
				if t ==i :
					t_list.remove(t)# the transaction holding the item has to be removed from locking transactions in lock table(shrinking phase)
				
				
			if len(waiting_t_list)== 0:
				continue # If there are no transaction waiting on the item then it proceeds to the next operation on the line
			
			
			first_waiting_trans = waiting_t_list.pop(0) # First transaction waiting in the queue is popped in order to put the transaction back to the active state
			#print(first_waiting_trans)
			trans1 = transaction_table[first_waiting_trans]
			op1 = trans1['waiting_operations'].pop(0) # Corresponding operation for the waiting transaction is being extracted
			
			if op1['operation'][0] == "r":
				read_item(op1['operation'])
			
			
			elif op1['operation'][0] == "w":
				if len(executing_lock['locking_transactions']) > 1:
					executing_lock['waiting_transactions'].append(first_waiting_trans)
				
				elif len(executing_lock['locking_transactions'])== 0 :
					write_item(op1['operation'])
			
			elif op1['operation'][0] == "e":
				end_transaction(op1['operation'][1],"committed",op1['operation'])
				
		executing_transactions['transaction_item'].clear() # Since the current transaction list varies for each transaction id it is cleared on each iteration


	except IndexError as e:
		e = sys.exc_info()[0]

		
				
def main():
	global ts
	filename="input.txt"
	file = open(filename ,"r") 
	
	for line in file: 
		transaction_id = line[1]
		if line[0] == "b" or line[0] == "e":
			
			
			if line[0] == "b":
				ts = ts + 1 
				transaction_table[transaction_id] = {'transaction_state': "active", 'Timestamp': ts, 'waiting_operations': [], 'transaction_item' : [], 'transaction_phase': True}
				print("\nExecuting " +line+ "\nChanges in Tables: New transaction is added to the transaction table with Transaction ID: " + str(transaction_id))

			elif line[0] == "e":
				
				if transaction_id in transaction_table:
				
					if transaction_table[transaction_id]['transaction_state'] == 'blocked': # Since the transaction is already blocked end_transaction cannot be performed
						transaction_table[transaction_id]['waiting_operations'].append({'operation':line, 'item_id' : 'N.A.'})
						print("\nExecuting " + line + "\nChanges in Tables: Since Transaction "+ str(transaction_id) + " is already blocked, e" + str(transaction_id) + " is added to list of waiting operations")

					elif transaction_table[transaction_id]['transaction_state'] == 'aborted': # Since this transaction is aborted, the items are waiting to be released.
						print("\nExecuting " + line + "\nChanges in Tables: Transaction " + str(transaction_id) +" is already aborted, its current Transaction state remains: " + str(transaction_table[transaction_id]['transaction_state']))

					else:
						commit_transaction = "committed"
						print("\nExecuting " + line + "\nChanges in Tables: Transaction " + str(transaction_id) + " is committed, and the status is updated to " + str(commit_transaction) + ". The items to be unlocked: " + str(transaction_table[transaction_id]['transaction_item']))
						end_transaction(transaction_id,commit_transaction,line) #  end_transaction is called recursively and the transaction is thereby committed
				

		elif line[0] == "r" or line[0] == "w":
			item_id = line[3]
			transaction_id = line[1]
			
			if transaction_id in transaction_table:
				
				if transaction_table[transaction_id]['transaction_state'] == 'blocked': # If the transaction is blocked it has to be updated in lock table waiting transaction column for an item and waiting operation's list in TT for that transaction
					transaction_table[transaction_id]['waiting_operations'].append({'operation':line, 'item_id':item_id}) # The current operatrion is newly added to the list of waiting opeartions in the transaction table for that transaction
					lock_table[item_id]['waiting_transactions'].append(transaction_id) # This transaction is updated in the list of waiting transactions in the lock table for that item
					
					
				elif transaction_table[transaction_id]['transaction_state'] == 'aborted':
					print("\nExecuting " + line + "\nChanges in Tables: transaction " + str(transaction_id) + " is already aborted, and the state remains " + str(transaction_table[transaction_id]['transaction_state']) + " and no futher operations can take place for this transaction")

				elif transaction_table[transaction_id]['transaction_state'] == 'active': # The read or write operations can only be performed in a transaction only when the transaction is in active state
					if line[0] == "r":
						read_item(line) 
					if line[0] == "w":
						write_item(line)
						
	print()
	print("Transaction Table:")
	print()
	print(transaction_table)
	print()
	print("Lock Table:")
	print()
	print(lock_table)
	
	
	
main()	