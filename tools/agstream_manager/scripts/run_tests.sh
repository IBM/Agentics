#!/bin/bash

# AGStream Manager Test Runner
# Runs all tests in the tests/ directory

set -e

# Get the script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AGSTREAM_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
TESTS_DIR="$AGSTREAM_DIR/tests"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
print_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
print_error() { echo -e "${RED}[ERROR]${NC} $1"; }
print_header() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}"
}

# Check if AGStream Manager service is running
check_service() {
    if ! curl -s http://localhost:5003/health > /dev/null 2>&1; then
        print_warn "AGStream Manager service is not running on port 5003"
        print_warn "Some tests may fail. Start the service with:"
        print_warn "  ./scripts/manage_agstream.sh start"
        echo ""
        read -p "Continue anyway? (y/N) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    else
        print_info "AGStream Manager service is running"
    fi
}

# Run all tests
run_all_tests() {
    print_header "Running AGStream Manager Tests"

    cd "$TESTS_DIR"

    local passed=0
    local failed=0
    local total=0

    for test_file in test_*.py; do
        if [ -f "$test_file" ]; then
            total=$((total + 1))
            echo ""
            print_info "Running: $test_file"
            echo "----------------------------------------"

            if python3 "$test_file"; then
                passed=$((passed + 1))
                echo -e "${GREEN}✓ PASSED${NC}"
            else
                failed=$((failed + 1))
                echo -e "${RED}✗ FAILED${NC}"
            fi
        fi
    done

    echo ""
    print_header "Test Results"
    echo -e "Total:  $total"
    echo -e "${GREEN}Passed: $passed${NC}"
    if [ $failed -gt 0 ]; then
        echo -e "${RED}Failed: $failed${NC}"
    else
        echo -e "Failed: $failed"
    fi

    if [ $failed -eq 0 ]; then
        echo ""
        print_info "All tests passed! 🎉"
        return 0
    else
        echo ""
        print_error "Some tests failed"
        return 1
    fi
}

# Run specific test
run_specific_test() {
    local test_name=$1
    cd "$TESTS_DIR"

    if [ ! -f "$test_name" ]; then
        print_error "Test file not found: $test_name"
        exit 1
    fi

    print_header "Running: $test_name"
    python3 "$test_name"
}

# List available tests
list_tests() {
    print_header "Available Tests"
    cd "$TESTS_DIR"

    local count=0
    for test_file in test_*.py; do
        if [ -f "$test_file" ]; then
            count=$((count + 1))
            echo "  $count. $test_file"
        fi
    done

    echo ""
    echo "Total: $count tests"
}

# Main
case "${1:-}" in
    all)
        check_service
        run_all_tests
        ;;
    list)
        list_tests
        ;;
    run)
        if [ -z "${2:-}" ]; then
            print_error "Please specify a test file"
            echo "Usage: $0 run <test_file.py>"
            exit 1
        fi
        check_service
        run_specific_test "$2"
        ;;
    *)
        echo "AGStream Manager Test Runner"
        echo ""
        echo "Usage: $0 {all|list|run <test_file>}"
        echo ""
        echo "Commands:"
        echo "  all              - Run all tests"
        echo "  list             - List available tests"
        echo "  run <test_file>  - Run a specific test"
        echo ""
        echo "Examples:"
        echo "  $0 all"
        echo "  $0 list"
        echo "  $0 run test_schema_manager.py"
        exit 1
        ;;
esac
