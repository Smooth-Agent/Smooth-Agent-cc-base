# Example: Mitra-style customer image extending smoothagent/cc-base.
#
# Build:  docker build -f examples/customer.dockerfile -t yourorg/your-builder:v1 .
# Push:   docker push yourorg/your-builder:v1
# Use:    register the pushed image at your SmoothAgent agent config.

FROM smoothagent/cc-base:latest

# Add the tools your agent needs.
USER root
RUN npm install --global --no-fund --no-audit \
    vite \
    typescript \
    @your-org/your-sdk
USER agent

# Drop in your scaffolds (read-only). The agent can copy from here into /workspace.
COPY --chown=agent:agent scaffolds/ /scaffolds/

# Defaults the build mode will pick up.
ENV BUILD_CMD="npm run build" \
    PROJECT_TYPE="react"

# Do NOT override ENTRYPOINT, USER, or WORKDIR — the contract depends on them.
