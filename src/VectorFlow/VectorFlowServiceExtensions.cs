using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace VectorFlow;

public static class VectorFlowServiceExtensions
{
    /// <summary>
    /// Registers VectorFlowClient as a singleton with the provided configuration.
    /// </summary>
    public static IServiceCollection AddVectorFlow(
        this IServiceCollection services,
        Action<VectorFlowOptions> configure)
    {
        var options = new VectorFlowOptions();
        configure(options);

        services.AddSingleton(options);
        services.AddSingleton(sp =>
        {
            var logger = sp.GetService<ILogger<VectorFlowClient>>();
            return new VectorFlowClient(options, logger);
        });

        return services;
    }
}
