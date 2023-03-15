package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {
    @Autowired
    private RestHighLevelClient client;

    @Override
    public PageResult search(RequestParams params) {
        SearchRequest request = new SearchRequest("hotel");

        buildQuery(params, request);

        int page = params.getPage();
        int size = params.getSize();
        request.source().from((page-1)*size).size(size);

        String location = params.getLocation();
        if (location != null && !location.equals("")) {
            request.source().sort(SortBuilders.geoDistanceSort("location", new GeoPoint(location))
                    .order(SortOrder.ASC).unit(DistanceUnit.KILOMETERS));
        }

        try {
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            return handleResponse(response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, List<String>> filters(RequestParams params) {
        SearchRequest request = new SearchRequest("hotel");

        buildQuery(params, request);
        request.source().size(0);
        buildAggregationRequest(request);
        try {
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            Map<String, List<String>> result = new HashMap<>();
            Aggregations aggregations = response.getAggregations();
            List<String> brandList = getAggByName(aggregations, "brand_agg");
            result.put("brand", brandList);
            List<String> cityList = getAggByName(aggregations, "city_agg");
            result.put("city", cityList);
            List<String> starNameList = getAggByName(aggregations, "starName_agg");
            result.put("starName", starNameList);
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private List<String> getAggByName(Aggregations aggregations, String aggName) {
        Terms brandTerm = aggregations.get(aggName);
        List<? extends Terms.Bucket> buckets = brandTerm.getBuckets();
        List<String> list = new ArrayList<>();
        for (Terms.Bucket bucket : buckets) {
            String brand = bucket.getKeyAsString();
            list.add(brand);
        }
        return list;
    }

    private void buildAggregationRequest(SearchRequest request) {
        request.source().aggregation(
                AggregationBuilders.terms("brand_agg")
                        .field("brand").size(30)
        );
        request.source().aggregation(
                AggregationBuilders.terms("city_agg")
                        .field("city").size(30)
        );
        request.source().aggregation(
                AggregationBuilders.terms("starName_agg")
                        .field("starName").size(30)
        );
    }

    private void buildQuery(RequestParams params, SearchRequest request) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        String key = params.getKey();
        if (key == null || "".equals(key)) {
            boolQuery.must(QueryBuilders.matchAllQuery());
        } else {
            boolQuery.must(QueryBuilders.matchQuery("all", key));
        }
        String city = params.getCity();
        if (city != null && !city.equals("")) {
            boolQuery.filter(QueryBuilders.termQuery("city", city));
        }
        String brand = params.getBrand();
        if (brand != null && !brand.equals("")) {
            boolQuery.filter(QueryBuilders.termQuery("brand", brand));
        }
        String starName = params.getStarName();
        if (starName != null && !starName.equals("")) {
            boolQuery.filter(QueryBuilders.termQuery("starName", starName));
        }
        if (params.getMinPrice() != null) {
            boolQuery.filter(QueryBuilders.rangeQuery("price").gte(params.getMinPrice()));
        }
        if (params.getMaxPrice() != null) {
            boolQuery.filter(QueryBuilders.rangeQuery("price").lte(params.getMaxPrice()));
        }

        FunctionScoreQueryBuilder functionScoreQuery =
                QueryBuilders.functionScoreQuery(
                        // 原始查询
                        boolQuery,
                        // function score array
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                                // a function score
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                        // 过滤条件
                                        QueryBuilders.termQuery("isAD", true),
                                        // 算分函数
                                        ScoreFunctionBuilders.weightFactorFunction(10)
                                )
                        });

        request.source().query(functionScoreQuery);
    }

    private PageResult handleResponse(SearchResponse response) {
        // 解析响应
        SearchHits searchHits = response.getHits();
        long total = searchHits.getTotalHits().value;
        // System.out.println("total: " + total + " records");
        // 文档数组
        SearchHit[] hits = searchHits.getHits();
        List<HotelDoc> hotels = new ArrayList<>();
        for (SearchHit hit : hits) {
            String string = hit.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(string, HotelDoc.class);
            Object[] values = hit.getSortValues();
            if (values.length > 0) {
                Object value = values[0];
                hotelDoc.setDistance(value);
            }
            hotels.add(hotelDoc);
        }
        return new PageResult(total, hotels);
    }

    @Override
    public List<String> getSuggestion(String prefix) {
        SearchRequest request = new SearchRequest("hotel");
        request.source().suggest(new SuggestBuilder().addSuggestion(
                "suggestions",
                SuggestBuilders.completionSuggestion("suggestion")
                        .prefix(prefix)
                        .skipDuplicates(true)
                        .size(10)
        ));
        try {
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            Suggest suggest = response.getSuggest();
            CompletionSuggestion completions = suggest.getSuggestion("suggestions");
            List<String> suggestions = new ArrayList<>(completions.getOptions().size());
            for (CompletionSuggestion.Entry.Option option : completions.getOptions()) {
                suggestions.add(option.getText().toString());
            }
            return suggestions;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void insertById(Long id) {
        try {
            Hotel hotel = getById(id);
            HotelDoc hotelDoc = new HotelDoc(hotel);
            IndexRequest request = new IndexRequest("hotel").id(hotelDoc.getId().toString());
            request.source(JSON.toJSONString(hotelDoc), XContentType.JSON);
            client.index(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteById(Long id) {
        try {
            DeleteRequest request = new DeleteRequest("hotel");
            client.delete(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
