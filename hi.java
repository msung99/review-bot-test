package moheng.applicationrunner.dev;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import moheng.applicationrunner.dto.LiveInformationRunner;
import moheng.liveinformation.domain.LiveInformation;
import moheng.liveinformation.domain.repository.LiveInformationRepository;
import moheng.liveinformation.domain.TripLiveInformation;
import moheng.liveinformation.domain.repository.TripLiveInformationRepository;
import moheng.liveinformation.exception.NoExistLiveInformationException;
import moheng.trip.domain.Trip;
import moheng.trip.domain.repository.TripRepository;
import moheng.trip.exception.NoExistTripException;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

@Order(3)
@Component
public class LiveInformationDevApplicationRunner implements ApplicationRunner {
    private final TripRepository tripRepository;
    private final LiveInformationRepository liveInformationRepository;
    private final TripLiveInformationRepository tripLiveInformationRepository;
    private final JdbcTemplate jdbcTemplate;

    public LiveInformationDevApplicationRunner(final TripRepository tripRepository,
                                               final LiveInformationRepository liveInformationRepository,
                                               final TripLiveInformationRepository tripLiveInformationRepository,
                                               final JdbcTemplate jdbcTemplate) {
        this.tripRepository = tripRepository;
        this.liveInformationRepository = liveInformationRepository;
        this.tripLiveInformationRepository = tripLiveInformationRepository;
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        if (liveInformationRepository.count() == 0) {
            final Resource resource = new ClassPathResource("json/liveinformation.json");
            final ObjectMapper objectMapper = new ObjectMapper();
            final List<LiveInformationRunner> liveInformationRunners = objectMapper.readValue(resource.getInputStream(), new TypeReference<List<LiveInformationRunner>>() {});

            final List<Object[]> tripLiveInformationBatchArgs = new ArrayList<>();
            final List<Object[]> liveInformationBatchArgs = new ArrayList<>();

            for (final LiveInformationRunner liveInformationRunner : liveInformationRunners) {
                final Trip trip = tripRepository.findByContentId(liveInformationRunner.getContentid())
                        .orElseThrow(NoExistTripException::new);

                for (final String liveInfoName : liveInformationRunner.getLiveinformation()) {
                    final LiveInformation liveInformation = findOrCreateLiveInformation(liveInfoName, liveInformationBatchArgs);
                    tripLiveInformationBatchArgs.add(new Object[]{liveInformation.getId(), trip.getId(), LocalDate.now(), LocalDate.now()});
                }
            }

            final String liveInformationSql = "INSERT INTO live_information (name, created_at, updated_at) VALUES (?, ?, ?)";
            if (!liveInformationBatchArgs.isEmpty()) {
                jdbcTemplate.batchUpdate(liveInformationSql, liveInformationBatchArgs);
            }

            final String tripLiveInformationSql = "INSERT INTO trip_live_information (live_information_id, trip_id, created_at, updated_at) VALUES (?, ?, ?, ?)";
            if (!tripLiveInformationBatchArgs.isEmpty()) {
                jdbcTemplate.batchUpdate(tripLiveInformationSql, tripLiveInformationBatchArgs);
            }
        }
    }

    private LiveInformation findOrCreateLiveInformation(final String liveInfoName, List<Object[]> liveInformationBatchArgs) {
        if (liveInformationRepository.existsByName(liveInfoName)) {
            return liveInformationRepository.findByName(liveInfoName)
                    .orElseThrow(NoExistLiveInformationException::new);
        } else {
            liveInformationBatchArgs.add(new Object[]{liveInfoName, LocalDate.now(), LocalDate.now()});
            return new LiveInformation(liveInfoName);
        }
    }
}
