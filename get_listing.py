BASE_URL = 'https://platform.realestate.co.nz/search/v1/listings'

# types of listings:
#[res_sale, com_sale, rural_sale, biz_sale, res_rent, com_lease, lifestyle_lease, lifestyle_sale]

# get all residential sales
#https://platform.realestate.co.nz/search/v1/listings?filter%5Bcategory%5D%5B%5D=res_sale

#&filter%5BexcludeListingId%5D%5B%5D=42676360&filter%5BgeoPointLat%5D=-37.7840829&filter%5BgeoPointLng%5D=175.2374799&filter%5BgeoRadius%5D=5&filter%5BbedroomsMin%5D=3&filter%5BpublishedAfter%5D=Wed%20Nov%2015%202023%2015%3A15%3A12%20GMT%2B1300%20(New%20Zealand%20Daylight%20Time)&page%5Boffset%5D=0&page%5Blimit%5D=10&page%5BgroupBy%5D=distance