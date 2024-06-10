student_details=LOAD 'student.txt' USING PigStorage(',') as (id:int,firstname:chararray,lastname:chararray,age:int,
phonenumber:chararray,
city:chararray);
filter_data=FILTER student_details BY city=='Chennai';
group_data=GROUP student_details by age;
STORE filter_data INTO 'filter';
STORE group_data INTO 'group';
