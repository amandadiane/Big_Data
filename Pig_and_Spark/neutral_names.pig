-- load the data by specifying the input file in the command line
Data = LOAD '$input' using PigStorage(',') AS
                (state:chararray,
                gender:chararray,
                year:int,
                name:chararray,
                number:int);


-- determine all male names and respective counts
male_names = FILTER Data BY gender=='M';
male_names_groups = GROUP male_names BY name;
male_names_counts = FOREACH male_names_groups GENERATE group as m_name, SUM(male_names.number) as m_co$


-- determine all female names and respective counts
female_names = FILTER Data BY gender=='F';
female_names_groups = GROUP female_names BY name;
female_names_counts = FOREACH female_names_groups GENERATE group as f_name, SUM(female_names.number) a$


-- join the two relations on baby name
join_names = JOIN male_names_counts BY m_name, female_names_counts BY f_name;


-- add ratio of male to female names, and total count, to relation
-- then filter the joined relation to when the ratio m_names:f_names is within [0.25,4]
add_ratio = FOREACH join_names GENERATE (f_name, 'F'), f_count, (m_name, 'M'), m_count,
                                (int) m_count+f_count AS total, (float) m_count/f_count AS ratio;
fltr_names = FILTER add_ratio BY (ratio > .25) AND (ratio < 4);


-- sort final answer by total count of names in descending order
sorted_neutral_names = ORDER fltr_names BY total DESC;


dump sorted_neutral_names;