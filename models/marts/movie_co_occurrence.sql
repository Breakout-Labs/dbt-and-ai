with ratings as (
    select
        user_id,
        movie_id
    from {{ source('movies', 'ratings') }}
),

movie_pairs as (
    select distinct
        least(r1.movie_id, r2.movie_id) as movie_id_1,
        greatest(r1.movie_id, r2.movie_id) as movie_id_2,
        r1.user_id
    from ratings as r1
    inner join ratings as r2
        on r1.user_id = r2.user_id
       and r1.movie_id < r2.movie_id
),

co_occurrence as (
    select
        movie_id_1,
        movie_id_2,
        count(distinct user_id) as distinct_user_count
    from movie_pairs
    group by 1, 2
),

movies as (
    select
        movie_id,
        title
    from {{ source('movies', 'movies') }}
)

select
    co_occurrence.movie_id_1,
    movie_1.title as movie_title_1,
    co_occurrence.movie_id_2,
    movie_2.title as movie_title_2,
    co_occurrence.distinct_user_count
from co_occurrence
left join movies as movie_1
    on co_occurrence.movie_id_1 = movie_1.movie_id
left join movies as movie_2
    on co_occurrence.movie_id_2 = movie_2.movie_id
