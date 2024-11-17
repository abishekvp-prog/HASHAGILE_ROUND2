from elasticsearch import Elasticsearch
import pandas as pd
import json
from elasticsearch.helpers import bulk

class ElasticsearchOperations:
    def __init__(self, host='localhost', port=8989, username='elastic', password='aoGsjNl5V64CIWhIiY7B'):
        
        
        self.es = Elasticsearch(
            [{'host': host, 'port': port, 'scheme': 'http'}],
            basic_auth=(username, password)
        )
        self.file_path = r"C:/Users/91861/Desktop/hashagile/archive/Employee Sample Data 1.csv"

    
        
    def createCollection(self, p_collection_name):
        
        p_collection_name = p_collection_name.lower().replace(' ', '_')
        
        mapping = {
            "mappings": {
                "properties": {
                    "Employee ID": {"type": "keyword"},
                    "First Name": {"type": "text"},
                    "Last Name": {"type": "text"},
                    "Gender": {"type": "keyword"},
                    "Department": {"type": "keyword"},
                    "Job Title": {"type": "text"},
                    "Email": {"type": "keyword"},
                    "Phone Number": {"type": "keyword"},
                    "Salary": {"type": "float"}
                }
            }
        }
        
        try:
            if self.es.indices.exists(index=p_collection_name):
                self.es.indices.delete(index=p_collection_name)
                print(f"Existing collection {p_collection_name} deleted.")
                
            response = self.es.indices.create(
                index=p_collection_name,
                body=mapping
            )
            print(f"Collection {p_collection_name} created successfully")
            return {"status": "success", "message": f"Collection {p_collection_name} created successfully"}
        except Exception as e:
            print(f"Error creating collection: {str(e)}")
            return {"status": "error", "message": str(e)}

    def indexData(self, p_collection_name, p_exclude_column):
        
        try:
            p_collection_name = p_collection_name.lower().replace(' ', '_')
            
            
            print(f"Reading data from {self.file_path}")
            df = pd.read_csv(self.file_path, encoding='latin1')
            print(f"Columns in CSV: {df.columns.tolist()}")
            
            
            if p_exclude_column in df.columns:
                df = df.drop(columns=[p_exclude_column])
                print(f"Excluded column: {p_exclude_column}")
            
            
            documents = df.to_dict('records')
            print(f"Number of documents to index: {len(documents)}")
            
            
            bulk_data = []
            for doc in documents:
                
                doc = {k: ('' if pd.isna(v) else v) for k, v in doc.items()}
                bulk_data.append({
                    '_index': p_collection_name,
                    '_id': str(doc['Employee ID']),
                    '_source': doc
                })
            
            
            if bulk_data:
                success, failed = bulk(self.es, bulk_data, refresh=True)
                print(f"Successfully indexed {success} documents")
                if failed:
                    print(f"Failed to index {len(failed)} documents")
            
            print(f"Data indexed successfully in {p_collection_name}")
            
            
            count = self.es.count(index=p_collection_name)['count']
            print(f"Total documents in index after indexing: {count}")
            
            return {"status": "success", "message": f"Data indexed successfully in {p_collection_name}"}
        except Exception as e:
            print(f"Error indexing data: {str(e)}")
            return {"status": "error", "message": str(e)}

    def searchByColumn(self, p_collection_name, p_column_name, p_column_value):
        """Search for records matching column value"""
        try:
            p_collection_name = p_collection_name.lower().replace(' ', '_')
            query = {
                "query": {
                    "match": {
                        p_column_name: p_column_value
                    }
                }
            }
            
            print(f"Executing search query: {json.dumps(query, indent=2)}")
            response = self.es.search(
                index=p_collection_name,
                body=query,
                size=100  
            )
            
            hits = response['hits']['hits']
            results = [hit['_source'] for hit in hits]
            print(f"Found {len(results)} matching records")
            
            
            if results:
                print("Sample results:")
                for i, result in enumerate(results[:3]):
                    print(f"Result {i+1}: {result}")
                    
            return {"status": "success", "results": results}
        except Exception as e:
            print(f"Error searching data: {str(e)}")
            return {"status": "error", "message": str(e)}

    def getEmpCount(self, p_collection_name):
        """Get total number of employees in collection"""
        try:
            p_collection_name = p_collection_name.lower().replace(' ', '_')
            response = self.es.count(index=p_collection_name)
            count = response['count']
            print(f"Total employees in {p_collection_name}: {count}")
            return {"status": "success", "count": count}
        except Exception as e:
            print(f"Error getting count: {str(e)}")
            return {"status": "error", "message": str(e)}

    def delEmpById(self, p_collection_name, p_employee_id):
        """Delete employee by ID"""
        try:
            p_collection_name = p_collection_name.lower().replace(' ', '_')
            
            
            exists = self.es.exists(index=p_collection_name, id=p_employee_id)
            if not exists:
                print(f"Employee {p_employee_id} not found in {p_collection_name}")
                return {"status": "error", "message": "Employee not found"}
            
            response = self.es.delete(
                index=p_collection_name,
                id=p_employee_id
            )
            print(f"Employee {p_employee_id} deleted successfully from {p_collection_name}")
            return {"status": "success", "message": f"Employee {p_employee_id} deleted successfully"}
        except Exception as e:
            print(f"Error deleting employee: {str(e)}")
            return {"status": "error", "message": str(e)}

    def getDepFacet(self, p_collection_name):
        
        try:
            p_collection_name = p_collection_name.lower().replace(' ', '_')
        
        # Check if the 'Department.keyword' field exists
            mapping = self.es.indices.get_mapping(index=p_collection_name)
            if 'Department.keyword' not in mapping[p_collection_name]['mappings']['properties']:
                print("Field 'Department.keyword' not found, using 'Department' instead.")
                department_field = "Department"
            else:
                department_field = "Department.keyword"
        
            query = {
                "size": 0,
                "aggs": {
                    "department_count": {
                        "terms": {
                            "field": department_field,
                            "size": 100
                        }
                    }
                }
            }
        
            print(f"Executing aggregation query: {json.dumps(query, indent=2)}")
            response = self.es.search(
                index=p_collection_name,
                body=query
            )
        
            if 'aggregations' in response:
                facets = response['aggregations']['department_count']['buckets']
                print("\nDepartment-wise employee count:")
                for facet in facets:
                    print(f"{facet['key']}: {facet['doc_count']} employees")
                return {"status": "success", "message": "Aggregation completed", "facets": facets}
            else:
                print("No aggregation results found.")
                return {"status": "error", "message": "No aggregation results found", "facets": None}
    
        except Exception as e:
            print(f"Error getting department facets: {str(e)}")
            return {"status": "error", "message": str(e), "facets": None}

def main():
    
    es_ops = ElasticsearchOperations(
        port=8989,
        username='elastic',
        password='aoGsjNl5V64CIWhIiY7B'
    )
    
    
    v_nameCollection = 'Hash_AbishekVP'
    v_phoneCollection = 'Hash_0871'
    
    print("\n=== Starting Elasticsearch Operations ===\n")
    
    # Step a
    print("\na) Setting v_nameCollection = 'Hash_AbishekVP'...")
    print(f"v_nameCollection = {v_nameCollection}")
    
    # Step b
    print("\nb) Setting v_phoneCollection = 'Hash_0871'...")
    print(f"v_phoneCollection = {v_phoneCollection}")
    
    # Step c
    print("\nc) Creating collection 'Hash_AbishekVP'...")
    es_ops.createCollection(v_nameCollection)
    
    # Step d
    print("\nd) Creating collection 'Hash_0871'...")
    es_ops.createCollection(v_phoneCollection)
    
    # Step e
    print("\ne) Getting initial employee count for 'Hash_AbishekVP'...")
    es_ops.getEmpCount(v_nameCollection)
    
    # Step f
    print("\nf) Indexing data in 'Hash_AbishekVP' on column 'Department'...")
    es_ops.indexData(v_nameCollection, 'Department')
    
    # Step g
    print("\ng) Indexing data in 'Hash_0871' on column 'Gender'...")
    es_ops.indexData(v_phoneCollection, 'Gender')
    
    # Step h
    print("\nh) Getting updated employee count for 'Hash_AbishekVP'...")
    es_ops.getEmpCount(v_nameCollection)
    
    # Step i
    print("\ni) Deleting employee with ID 'E02003' from 'Hash_AbishekVP'...")
    es_ops.delEmpById(v_nameCollection, 'E02003')
    
    # Step j
    print("\nj) Getting employee count for 'Hash_AbishekVP' after deletion...")
    es_ops.getEmpCount(v_nameCollection)
    
    # Step k
    print("\nk) Searching 'Hash_AbishekVP' for employees in Department 'IT'...")
    es_ops.searchByColumn(v_nameCollection, 'Department', 'IT')
    
    # Step l
    print("\nl) Searching 'Hash_AbishekVP' for employees with Gender 'Male'...")
    es_ops.searchByColumn(v_nameCollection, 'Gender', 'Male')
    
    # Step m
    print("\nm) Searching 'Hash_0871' for employees in Department 'IT'...")
    es_ops.searchByColumn(v_phoneCollection, 'Department', 'IT')
    
    # Step n
    print("\nn) Getting department facets for 'Hash_AbishekVP'...")
    es_ops.getDepFacet(v_nameCollection)
    
    # Step o
    print("\no) Getting department facets for 'Hash_0871'...")
    es_ops.getDepFacet(v_phoneCollection)
    
    print("\n=== Completed All Operations ===")



if __name__ == "__main__":
    main()
