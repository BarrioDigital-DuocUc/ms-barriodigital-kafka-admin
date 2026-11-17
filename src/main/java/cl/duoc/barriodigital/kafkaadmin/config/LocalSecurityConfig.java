package cl.duoc.barriodigital.kafkaadmin.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.web.SecurityFilterChain;

/**
 * Sin auth propia: el BFF aplica JWT + rol Admin.
 * Activo en todos los perfiles (local/prod/demo).
 */
@Configuration
public class LocalSecurityConfig {

    @Bean
    public SecurityFilterChain localSecurityFilterChain(
            HttpSecurity http) throws Exception {

        http
                .csrf(AbstractHttpConfigurer::disable)
                .authorizeHttpRequests(auth -> auth
                        .requestMatchers(
                                "/api/v1/kafka-admin/**",
                                "/actuator/health",
                                "/actuator/info"
                        ).permitAll()
                        .anyRequest().denyAll()
                );

        return http.build();
    }
}
