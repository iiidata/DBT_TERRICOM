{{ config(materialized='table') }}


select rating_count, 
       new_like_count / 31 as like_moyen_par_jour, 
       followers_count as nombre_followers, 
       were_here_count

from public.page