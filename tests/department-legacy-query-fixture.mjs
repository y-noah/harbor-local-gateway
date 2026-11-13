// Admission/read-sharing unit fixtures represent a proven pre-department
// database. Real migration and authority checks live in the PostgreSQL tests.
export const withLegacyDepartmentSchema=store=>({...store,get:async(sql,...args)=>{
 if(sql.includes("to_regclass('harbor.gateway_settings')"))return {settings:true,departments:false,admin_keys:false,department_settings:false,columns:false};
 if(sql==='SELECT value FROM harbor.gateway_settings WHERE name=$1'&&args[0]==='schema.department-isolation')return null;
 return store.get(sql,...args);
}});
