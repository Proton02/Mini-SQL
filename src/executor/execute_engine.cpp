#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"

extern "C" {
int yyparse(void);
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if ((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
  // /** When you have completed all the code for
  //  *  the test, run it using main.cpp and uncomment
  //  *  this part of the code.*/
  // struct dirent *stdir;
  // while((stdir = readdir(dir)) != nullptr) {
  //   if( strcmp( stdir->d_name , "." ) == 0 ||
  //       strcmp( stdir->d_name , "..") == 0 ||
  //       stdir->d_name[0] == '.')
  //     continue;
  //   dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  // }
  //  /**/
  // closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Txn *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
      int a = result_set->size();
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if (!current_db_.empty()) context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    return DB_ALREADY_EXIST;
  }
  dbs_.insert(make_pair(db_name, new DBStorageEngine(db_name, true)));
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) == dbs_.end()) {
    return DB_NOT_EXIST;
  }
  remove(("./databases/" + db_name).c_str());
  delete dbs_[db_name];
  dbs_.erase(db_name);
  if (db_name == current_db_) current_db_ = "";
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  if (dbs_.empty()) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_SUCCESS;
  }
  int max_width = 8;
  for (const auto &itr : dbs_) {
    if (itr.first.length() > max_width) max_width = itr.first.length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << "" << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << "Database" << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << "" << "+" << endl;
  for (const auto &itr : dbs_) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr.first << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << "" << "+" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    current_db_ = db_name;
    cout << "Database changed" << endl;
    return DB_SUCCESS;
  }
  return DB_NOT_EXIST;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  vector<TableInfo *> tables;
  if (dbs_[current_db_]->catalog_mgr_->GetTables(tables) == DB_FAILED) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_FAILED;
  }
  string table_in_db("Tables_in_" + current_db_);
  uint max_width = table_in_db.length();
  for (const auto &itr : tables) {
    if (itr->GetTableName().length() > max_width) max_width = itr->GetTableName().length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << "" << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << table_in_db << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << "" << "+" << endl;
  for (const auto &itr : tables) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr->GetTableName() << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << "" << "+" << endl;
  return DB_SUCCESS;
}

using namespace std;
/**
 * TODO: Student Implement
 */
// 访问 ExecuteContext来实现表的修改
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  // 当前是否选择数据库？
  if (current_db_.empty()) {
    return DB_FAILED;
  }else{
    string table_name = ast->child_->val_;
    // 获取表名 通过exec_ctx_获得Catalog调用GetTable()函数
    CatalogManager *ct_manager = context->GetCatalog();
    SyntaxNode *node = ast->child_->next_->child_;
    vector<Column *> clm;
    vector<string> pkey;  // primary
    vector<string> ukey;  // unique
    int index = 0;
    for(int index = 0; node != nullptr; node = node->next_){
      // kNodeColumnDefinition,     column definition, contains column identifier and column type and UNIQUE constrain
      if(node->type_ == kNodeColumnDefinition){
        Column *column;
        string clm_name = node->child_->val_;
        string clm_type = node->child_->next_->val_;
        bool uniq = node->val_ != nullptr && string(node->val_) == "unique";
        if(uniq){
          ukey.push_back(clm_name);
        }
        if (clm_type == "int") {  // int类型
          column = new Column(clm_name, TypeId::kTypeInt, index++, false, uniq);
        }else if(clm_type == "char"){  // char类型 可以包含空值
          int length = stoi(node->child_->next_->child_->val_);
          column = new Column(clm_name, TypeId::kTypeChar, length, index++, true, uniq);
        }else if(clm_type == "float"){  // float类型 可以包含空值
          column = new Column(clm_name, TypeId::kTypeFloat, index++, true, uniq);
        } 
        clm.push_back(column);
      }// kNodeColumnDefinitionList, contains several column definitions
      else if(node->type_ == kNodeColumnList && string(node->val_) == "primary keys"){
        SyntaxNode *clm_list = node->child_;
        while(clm_list != nullptr) {
          pkey.push_back(clm_list->val_);
          clm_list = clm_list->next_;
        }
      }
    }
    Schema *schema = new Schema(clm);
    TableInfo *tinfo;
    IndexInfo *iinfo;
    // 是否创建成功
    if(ct_manager->CreateTable(table_name, schema, context->GetTransaction(), tinfo) == DB_FAILED){
      return DB_FAILED;
    }
    string idx_name = "PRECREATED_INDEX_OF_";
    for (auto it:pkey){
      idx_name += it + "_";
    }
    idx_name += "ON_" + table_name;
    ct_manager->CreateIndex(table_name, idx_name, pkey, context->GetTransaction(), iinfo, "btree");
    for(auto it:ukey){
      idx_name = "PRECREATED_INDEX_OF_UNIQUE_" + it + "_ON_" + table_name;
      ct_manager->CreateIndex(table_name, idx_name, {it}, context->GetTransaction(), iinfo, "btree");
    }
    return DB_SUCCESS;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  CatalogManager *ct_manager = context->GetCatalog();
  string table_name = ast->child_->val_;
  TableInfo *tinfo;
  vector<IndexInfo *> idxs;
  // 数据库为空，或者catalog中drop失败
  if(current_db_.empty() || ct_manager->DropTable(table_name) == DB_FAILED){
    return DB_FAILED;
  }
  int dberr = ct_manager->GetTableIndexes(table_name, idxs);
  if(dberr == DB_SUCCESS){  // 多个索引
    for(auto it:idxs){
      dberr = ct_manager->DropIndex(table_name, it->GetIndexName());
      if(dberr == DB_FAILED) return DB_FAILED;
    }
  }
  return DB_SUCCESS;
}


/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if(current_db_.empty()){
    return DB_FAILED;
  }else{
    CatalogManager *ct_manager = context->GetCatalog();
    vector<TableInfo*> tinfo;
    vector<IndexInfo*> iinfo;
    ct_manager->GetTables(tinfo);
    for(auto it:tinfo){
      ct_manager->GetTableIndexes(it->GetTableName(), iinfo);
    }
    vector<int> width;
    width.push_back(2); // 最小搞个2
    stringstream ss;
    ResultWriter writer(ss);
    for(auto it:iinfo){
      width[0] = width[0] >= stoi(it->GetIndexName()) ? width[0] : stoi(it->GetIndexName());
    }
    writer.Divider(width);
    writer.BeginRow();
    writer.WriteHeaderCell("Index", width[0]);
    writer.EndRow();
    writer.Divider(width);
    for(auto it:iinfo){
      writer.BeginRow();
      writer.WriteCell(it->GetIndexName(), width[0]);
      writer.EndRow();
    }
    writer.Divider(width);
    std::cout<<writer.stream_.rdbuf();
    return DB_SUCCESS;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  if(current_db_.empty()){
    return DB_FAILED;
  }else{
    CatalogManager *ct_manager = context->GetCatalog();
    string idx_name = ast->child_->val_;
    string table_name = ast->child_->next_->val_;
    vector<string> idx_key;
    SyntaxNode *curr_node = ast->child_->next_->next_->child_;
    // 从ast子节点开始依次提取idx和table名字
    for(; curr_node != nullptr; curr_node = curr_node->next_){
      idx_key.push_back(curr_node->val_);
    }
    IndexInfo *iinfo;
    return ct_manager->CreateIndex(table_name, idx_name,idx_key, context->GetTransaction(), iinfo, "btree");
  }
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  if(current_db_.empty()){
    return DB_FAILED;
  }else{
    CatalogManager *ct_manager = context->GetCatalog();
    string idx_name = ast->child_->val_;
    vector<TableInfo*> tinfo;
    ct_manager->GetTables(tinfo);
    for(auto t:tinfo){
      int dberr = ct_manager->DropIndex(t->GetTableName(), idx_name);
      if(dberr == DB_SUCCESS)
        return DB_SUCCESS;
    }
    return DB_FAILED;
  }
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  string file_name(ast->child_->val_);
  fstream file;
  file.open(file_name);
  if(!file.is_open()){
    return DB_FAILED;
  }else{
    char command[1024];
    size_t affected_row = 0;
    double time = 0;
    stringstream ss;
    ResultWriter writer(ss);
    while(!file.eof()){
      memset(command, 0, 1024);
      int cnt;
      char curr_char;
      for(cnt = 0; !file.eof(); cnt++){
        curr_char = file.get();
        if(curr_char == ';')
          break;
        else
          command[cnt] = curr_char;
      }
      if(curr_char != ';')
        break;
      if(!file.eof())
        file.get();
      command[cnt] = curr_char;
      // 创建缓冲区,把命令command搞到缓冲区
      yy_buffer_state *buffer = yy_scan_string(command);
      MinisqlParserInit();
      yyparse();  // 启动语法解析过程
      dberr_t result = Execute(MinisqlGetParserRootNode());
      MinisqlParserFinish();
      yy_delete_buffer(buffer);
      yylex_destroy();
      if(result == DB_QUIT){
        writer.EndInformation(affected_row, time, false);
        return DB_QUIT;
      }
    }
    writer.EndInformation(affected_row,time,false);
    return DB_SUCCESS;
  }
}
/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  current_db_ = "";
  return DB_QUIT;
}

