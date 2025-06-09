#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the vLLM project
"""
Comprehensive tests for V1 FlashAttention implementation.

This test suite validates both interface compliance and mathematical
correctness:
1. Cross-attention support (ENCODER, ENCODER_DECODER, DECODER attention types)
2. Metadata validation and KV cache handling
3. Mathematical correctness and attention properties
4. Query sensitivity and numerical accuracy
"""

import pytest
import torch

from vllm.attention.layer import AttentionType
from vllm.v1.attention.backends.flash_attn import (FlashAttentionImpl,
                                                   FlashAttentionMetadata,
                                                   _get_causal_option,
                                                   _get_query_key_seq_metadata)


class TestFlashAttentionInterface:
    """Test suite for V1 FlashAttention interface compliance and
    cross-attention support."""

    @pytest.fixture
    def device(self):
        return torch.device("cuda" if torch.cuda.is_available() else "cpu")

    @pytest.fixture
    def attention_params(self):
        """Standard attention parameters for testing."""
        return {
            "num_heads": 8,
            "head_size": 64,
            "scale": 1.0 / (64**0.5),
            "num_kv_heads": 8,
            "alibi_slopes": None,
            "sliding_window": None,
            "kv_cache_dtype": "auto",
            "logits_soft_cap": None,
        }

    def create_test_tensors(self,
                            device,
                            batch_size=2,
                            seq_len=10,
                            num_heads=8,
                            head_size=64):
        """Create test query, key, value tensors."""
        num_tokens = batch_size * seq_len
        query = torch.randn(num_tokens,
                            num_heads,
                            head_size,
                            device=device,
                            dtype=torch.float16)
        key = torch.randn(num_tokens,
                          num_heads,
                          head_size,
                          device=device,
                          dtype=torch.float16)
        value = torch.randn(num_tokens,
                            num_heads,
                            head_size,
                            device=device,
                            dtype=torch.float16)
        return query, key, value

    def create_test_metadata(self,
                             device,
                             batch_size=2,
                             seq_len=10,
                             attn_type=AttentionType.DECODER):
        """Create test attention metadata for different attention types."""
        num_tokens = batch_size * seq_len

        # Basic metadata
        query_start_loc = torch.tensor([0, seq_len, num_tokens],
                                       dtype=torch.int32,
                                       device=device)
        seq_lens = torch.tensor([seq_len] * batch_size,
                                dtype=torch.int32,
                                device=device)
        slot_mapping = torch.arange(num_tokens,
                                    dtype=torch.long,
                                    device=device)
        block_table = torch.zeros((batch_size, 10),
                                  dtype=torch.int32,
                                  device=device)

        metadata = FlashAttentionMetadata(
            num_actual_tokens=num_tokens,
            max_query_len=seq_len,
            query_start_loc=query_start_loc,
            max_seq_len=seq_len,
            seq_lens=seq_lens,
            block_table=block_table,
            slot_mapping=slot_mapping,
            use_cascade=False,
            common_prefix_len=0,
            cu_prefix_query_lens=None,
            prefix_kv_lens=None,
            suffix_kv_lens=None,
        )

        # Add encoder/cross-attention specific metadata if needed
        if attn_type in [AttentionType.ENCODER, AttentionType.ENCODER_DECODER]:
            encoder_seq_len = 15  # Different from decoder seq_len
            encoder_tokens = batch_size * encoder_seq_len

            metadata.encoder_seq_lens = [encoder_seq_len] * batch_size
            metadata.encoder_seq_lens_tensor = torch.tensor([encoder_seq_len] *
                                                            batch_size,
                                                            dtype=torch.int32,
                                                            device=device)
            metadata.encoder_seq_start_loc = torch.tensor(
                [0, encoder_seq_len, encoder_tokens],
                dtype=torch.int32,
                device=device,
            )
            metadata.max_encoder_seq_len = encoder_seq_len
            metadata.num_encoder_tokens = encoder_tokens

            if attn_type == AttentionType.ENCODER_DECODER:
                metadata.cross_slot_mapping = torch.arange(encoder_tokens,
                                                           dtype=torch.long,
                                                           device=device)
                metadata.cross_block_tables = torch.zeros((batch_size, 10),
                                                          dtype=torch.int32,
                                                          device=device)

        return metadata

    def test_attention_type_support(self, device, attention_params):
        """Test that different attention types are now supported."""
        # Test ENCODER type
        encoder_impl = FlashAttentionImpl(attn_type=AttentionType.ENCODER,
                                          **attention_params)
        assert encoder_impl.attn_type == AttentionType.ENCODER

        # Test ENCODER_DECODER type
        cross_impl = FlashAttentionImpl(
            attn_type=AttentionType.ENCODER_DECODER, **attention_params)
        assert cross_impl.attn_type == AttentionType.ENCODER_DECODER

        # Test DECODER type (should still work)
        decoder_impl = FlashAttentionImpl(attn_type=AttentionType.DECODER,
                                          **attention_params)
        assert decoder_impl.attn_type == AttentionType.DECODER

    def test_causal_option_helper(self):
        """Test the _get_causal_option helper function."""
        assert _get_causal_option(AttentionType.DECODER)
        assert not _get_causal_option(AttentionType.ENCODER)
        assert not _get_causal_option(AttentionType.ENCODER_DECODER)
        assert not _get_causal_option(AttentionType.ENCODER_ONLY)

    def test_metadata_validation(self, device, attention_params):
        """Test that metadata validation works correctly for different
        attention types."""
        # Test encoder attention requires encoder metadata
        encoder_impl = FlashAttentionImpl(attn_type=AttentionType.ENCODER,
                                          **attention_params)

        # Create metadata without encoder fields - should fail
        incomplete_metadata = self.create_test_metadata(
            device, attn_type=AttentionType.DECODER)

        # Mock layer object
        class MockLayer:
            _k_scale = torch.tensor(1.0, device=device)
            _v_scale = torch.tensor(1.0, device=device)
            _q_scale = torch.tensor(1.0, device=device)

        layer = MockLayer()
        query, key, value = self.create_test_tensors(device)
        output = torch.zeros_like(query)
        kv_cache = torch.zeros((2, 10, 16, 8, 64),
                               device=device,
                               dtype=torch.float16)

        with pytest.raises(AttributeError,
                           match="Encoder attention requires setting"):
            encoder_impl.forward(layer, query, key, value, kv_cache,
                                 incomplete_metadata, output)

    def test_encoder_decoder_metadata_validation(self, device,
                                                 attention_params):
        """Test that encoder-decoder attention requires cross-attention
        metadata."""
        cross_impl = FlashAttentionImpl(
            attn_type=AttentionType.ENCODER_DECODER, **attention_params)

        # Create metadata with encoder fields but without cross-attention fields
        metadata = self.create_test_metadata(device,
                                             attn_type=AttentionType.ENCODER)

        # Don't set cross_slot_mapping and cross_block_tables

        class MockLayer:
            _k_scale = torch.tensor(1.0, device=device)
            _v_scale = torch.tensor(1.0, device=device)
            _q_scale = torch.tensor(1.0, device=device)

        layer = MockLayer()
        query, key, value = self.create_test_tensors(device)
        output = torch.zeros_like(query)
        kv_cache = torch.zeros((2, 10, 16, 8, 64),
                               device=device,
                               dtype=torch.float16)

        with pytest.raises(AttributeError,
                           match="requires setting cross-attention"):
            cross_impl.forward(layer, query, key, value, kv_cache, metadata,
                               output)

    def test_query_key_seq_metadata_helper(self, device):
        """Test the _get_query_key_seq_metadata helper function."""
        metadata = self.create_test_metadata(
            device, attn_type=AttentionType.ENCODER_DECODER)

        # Test decoder attention
        q_start, q_max, k_start, k_max = _get_query_key_seq_metadata(
            metadata, True, AttentionType.DECODER)
        assert torch.equal(q_start, metadata.query_start_loc)
        assert q_max == metadata.max_query_len
        assert torch.equal(k_start, metadata.query_start_loc)
        assert k_max == metadata.max_seq_len

        # Test encoder-decoder attention
        q_start, q_max, k_start, k_max = _get_query_key_seq_metadata(
            metadata, True, AttentionType.ENCODER_DECODER)
        assert torch.equal(q_start, metadata.query_start_loc)
        assert q_max == metadata.max_query_len
        assert torch.equal(k_start, metadata.encoder_seq_start_loc)
        assert k_max == metadata.max_encoder_seq_len

        # Test encoder attention
        q_start, q_max, k_start, k_max = _get_query_key_seq_metadata(
            metadata, True, AttentionType.ENCODER)
        assert torch.equal(q_start, metadata.encoder_seq_start_loc)
        assert q_max == metadata.max_encoder_seq_len
        assert torch.equal(k_start, metadata.encoder_seq_start_loc)
        assert k_max == metadata.max_encoder_seq_len

    @pytest.mark.skipif(not torch.cuda.is_available(),
                        reason="CUDA not available")
    def test_encoder_attention_forward(self, device, attention_params):
        """Test encoder self-attention forward pass."""
        if device.type != "cuda":
            pytest.skip("FlashAttention requires CUDA")

        try:
            # Test if flash attention is available
            import vllm.vllm_flash_attn

            assert hasattr(vllm.vllm_flash_attn, "flash_attn_varlen_func")
        except (ImportError, AttributeError):
            pytest.skip("FlashAttention package not available")

        encoder_impl = FlashAttentionImpl(attn_type=AttentionType.ENCODER,
                                          **attention_params)

        metadata = self.create_test_metadata(device,
                                             attn_type=AttentionType.ENCODER)

        class MockLayer:
            _k_scale = torch.tensor(1.0, device=device)
            _v_scale = torch.tensor(1.0, device=device)
            _q_scale = torch.tensor(1.0, device=device)

        layer = MockLayer()
        query, key, value = self.create_test_tensors(device)
        output = torch.zeros_like(query)
        kv_cache = torch.zeros((2, 10, 16, 8, 64),
                               device=device,
                               dtype=torch.float16)

        # Should not raise an exception
        result = encoder_impl.forward(layer, query, key, value, kv_cache,
                                      metadata, output)
        assert result is not None
        assert result.shape == output.shape

    def test_metadata_properties(self, device):
        """Test the metadata property validators."""
        # Test encoder metadata property
        metadata = self.create_test_metadata(device,
                                             attn_type=AttentionType.ENCODER)
        assert metadata.is_all_encoder_attn_metadata_set

        # Test missing encoder metadata
        incomplete_metadata = self.create_test_metadata(
            device, attn_type=AttentionType.DECODER)
        assert not incomplete_metadata.is_all_encoder_attn_metadata_set

        # Test cross-attention metadata property
        cross_metadata = self.create_test_metadata(
            device, attn_type=AttentionType.ENCODER_DECODER)
        assert cross_metadata.is_all_cross_attn_metadata_set

        # Test missing cross-attention metadata
        encoder_metadata = self.create_test_metadata(
            device, attn_type=AttentionType.ENCODER)
        assert not encoder_metadata.is_all_cross_attn_metadata_set


class TestFlashAttentionCorrectness:
    """Test suite for validating numerical correctness of attention
    operations."""

    @pytest.fixture
    def device(self):
        return torch.device("cuda" if torch.cuda.is_available() else "cpu")

    @pytest.fixture
    def attention_params(self):
        return {
            "num_heads": 2,  # Smaller for easier debugging
            "head_size": 32,  # Minimum supported head size by FlashAttention
            "scale": 1.0 / (32**0.5),  # Standard attention scaling
            "num_kv_heads": 2,
            "alibi_slopes": None,
            "sliding_window": None,
            "kv_cache_dtype": "auto",
            "logits_soft_cap": None,
        }

    def create_realistic_tensors(
        self,
        device,
        batch_size=1,
        seq_len=3,
        num_heads=2,
        head_size=32,
        seed=42,
    ):
        """Create realistic tensors with controlled randomness for
        reproducible testing."""
        torch.manual_seed(seed)

        # Create realistic random tensors scaled appropriately
        query = (torch.randn(
            batch_size * seq_len,
            num_heads,
            head_size,
            device=device,
            dtype=torch.float16,
        ) * 0.1)
        key = (torch.randn(
            batch_size * seq_len,
            num_heads,
            head_size,
            device=device,
            dtype=torch.float16,
        ) * 0.1)
        value = (torch.randn(
            batch_size * seq_len,
            num_heads,
            head_size,
            device=device,
            dtype=torch.float16,
        ) * 0.1)

        # Add some structure to make attention patterns meaningful
        # Make first token query align better with first token key
        query[0] = query[0] + 0.1 * key[0]
        # Make last token query align better with last token key
        query[-1] = query[-1] + 0.1 * key[-1]

        return query, key, value

    def create_simple_metadata(self,
                               device,
                               batch_size=1,
                               seq_len=3,
                               attn_type=AttentionType.DECODER):
        """Create simple metadata for testing."""
        num_tokens = batch_size * seq_len

        query_start_loc = torch.tensor([0, num_tokens],
                                       dtype=torch.int32,
                                       device=device)
        seq_lens = torch.tensor([seq_len] * batch_size,
                                dtype=torch.int32,
                                device=device)
        slot_mapping = torch.arange(num_tokens,
                                    dtype=torch.long,
                                    device=device)
        block_table = torch.zeros((batch_size, 10),
                                  dtype=torch.int32,
                                  device=device)

        metadata = FlashAttentionMetadata(
            num_actual_tokens=num_tokens,
            max_query_len=seq_len,
            query_start_loc=query_start_loc,
            max_seq_len=seq_len,
            seq_lens=seq_lens,
            block_table=block_table,
            slot_mapping=slot_mapping,
            use_cascade=False,
            common_prefix_len=0,
            cu_prefix_query_lens=None,
            prefix_kv_lens=None,
            suffix_kv_lens=None,
        )

        # Add encoder metadata if needed
        if attn_type in [AttentionType.ENCODER, AttentionType.ENCODER_DECODER]:
            encoder_seq_len = 2  # Different from decoder
            encoder_tokens = batch_size * encoder_seq_len

            metadata.encoder_seq_lens = [encoder_seq_len] * batch_size
            metadata.encoder_seq_lens_tensor = torch.tensor([encoder_seq_len] *
                                                            batch_size,
                                                            dtype=torch.int32,
                                                            device=device)
            metadata.encoder_seq_start_loc = torch.tensor([0, encoder_tokens],
                                                          dtype=torch.int32,
                                                          device=device)
            metadata.max_encoder_seq_len = encoder_seq_len
            metadata.num_encoder_tokens = encoder_tokens

            if attn_type == AttentionType.ENCODER_DECODER:
                metadata.cross_slot_mapping = torch.arange(encoder_tokens,
                                                           dtype=torch.long,
                                                           device=device)
                metadata.cross_block_tables = torch.zeros((batch_size, 10),
                                                          dtype=torch.int32,
                                                          device=device)

        return metadata

    @pytest.mark.skipif(not torch.cuda.is_available(),
                        reason="CUDA not available")
    def test_decoder_attention_correctness(self, device, attention_params):
        """Test that decoder self-attention produces correct results."""
        if device.type != "cuda":
            pytest.skip("FlashAttention requires CUDA")

        try:
            import vllm.vllm_flash_attn

            assert hasattr(vllm.vllm_flash_attn, "flash_attn_varlen_func")
        except (ImportError, AttributeError):
            pytest.skip("FlashAttention package not available")

        # Create implementation
        impl = FlashAttentionImpl(attn_type=AttentionType.DECODER,
                                  **attention_params)

        # Create test data
        query, key, value = self.create_realistic_tensors(device)
        metadata = self.create_simple_metadata(device)

        class MockLayer:
            _k_scale = torch.tensor(1.0, device=device)
            _v_scale = torch.tensor(1.0, device=device)
            _q_scale = torch.tensor(1.0, device=device)

        layer = MockLayer()
        output = torch.zeros_like(query)
        kv_cache = torch.zeros((2, 10, 16, 2, 32),
                               device=device,
                               dtype=torch.float16)

        # Run attention
        result = impl.forward(layer, query, key, value, kv_cache, metadata,
                              output)

        # Check that outputs are not all zeros (attention actually happened)
        assert not torch.allclose(result, torch.zeros_like(result)), (
            "Output should not be all zeros")

        # Check that different positions have different outputs
        # (causal masking effect)
        assert not torch.allclose(result[0], result[1]), (
            "Different positions should have different outputs")

        print("✅ Decoder attention produces non-trivial, "
              "position-dependent outputs")

    def test_causal_vs_non_causal_difference(self, device, attention_params):
        """Test that causal and non-causal attention produce different
        results."""
        if device.type != "cuda":
            pytest.skip("FlashAttention requires CUDA")

        try:
            import vllm.vllm_flash_attn

            assert hasattr(vllm.vllm_flash_attn, "flash_attn_varlen_func")
        except (ImportError, AttributeError):
            pytest.skip("FlashAttention package not available")

        # Test with decoder (causal) vs encoder (non-causal)
        decoder_impl = FlashAttentionImpl(attn_type=AttentionType.DECODER,
                                          **attention_params)
        encoder_impl = FlashAttentionImpl(attn_type=AttentionType.ENCODER,
                                          **attention_params)

        # Create identical inputs
        query, key, value = self.create_realistic_tensors(device)
        decoder_metadata = self.create_simple_metadata(
            device, attn_type=AttentionType.DECODER)
        encoder_metadata = self.create_simple_metadata(
            device, attn_type=AttentionType.ENCODER)

        class MockLayer:
            _k_scale = torch.tensor(1.0, device=device)
            _v_scale = torch.tensor(1.0, device=device)
            _q_scale = torch.tensor(1.0, device=device)

        layer = MockLayer()
        kv_cache = torch.zeros((2, 10, 16, 2, 32),
                               device=device,
                               dtype=torch.float16)

        # Run both attention types
        decoder_output = torch.zeros_like(query)
        encoder_output = torch.zeros_like(query)

        decoder_result = decoder_impl.forward(layer, query, key, value,
                                              kv_cache, decoder_metadata,
                                              decoder_output)
        encoder_result = encoder_impl.forward(layer, query, key, value,
                                              kv_cache, encoder_metadata,
                                              encoder_output)

        # Causal and non-causal should produce different results
        assert not torch.allclose(decoder_result, encoder_result, atol=1e-5), (
            "Causal and non-causal attention should produce different results")

        print("✅ Causal vs non-causal attention produce different outputs")

    def test_direct_tensor_query_sensitivity(self, device, attention_params):
        """Test that attention responds to query changes when using direct
        tensors (encoder mode)."""
        if device.type != "cuda":
            pytest.skip("FlashAttention requires CUDA")

        try:
            import vllm.vllm_flash_attn

            assert hasattr(vllm.vllm_flash_attn, "flash_attn_varlen_func")
        except (ImportError, AttributeError):
            pytest.skip("FlashAttention package not available")

        # Use encoder mode to test direct tensor path
        impl = FlashAttentionImpl(attn_type=AttentionType.ENCODER,
                                  **attention_params)

        # Create realistic test data
        query1, key, value = self.create_realistic_tensors(device, seed=42)
        query2 = query1.clone()
        query2[0] += 0.5  # Significant change to first query token

        metadata = self.create_simple_metadata(device,
                                               attn_type=AttentionType.ENCODER)

        class MockLayer:
            _k_scale = torch.tensor(1.0, device=device)
            _v_scale = torch.tensor(1.0, device=device)
            _q_scale = torch.tensor(1.0, device=device)

        layer = MockLayer()

        # Use proper KV cache structure with empty content
        # Format: [2, num_blocks, block_size, num_kv_heads, head_size]
        kv_cache = torch.zeros((2, 1, 16, 2, 32),
                               device=device,
                               dtype=torch.float16)

        output1 = torch.zeros_like(query1)
        output2 = torch.zeros_like(query2)

        result1 = impl.forward(layer, query1, key, value, kv_cache, metadata,
                               output1)
        result2 = impl.forward(layer, query2, key, value, kv_cache, metadata,
                               output2)

        # Query changes should produce different outputs when using direct
        # tensors
        max_diff = torch.max(torch.abs(result1 - result2)).item()
        assert max_diff > 1e-6, (
            f"Query changes should affect output (max diff: {max_diff})")

        print(f"✅ Direct tensor mode responds correctly to query changes "
              f"(max diff: {max_diff:.6f})")

    def test_attention_mathematical_properties(self, device, attention_params):
        """Test fundamental mathematical properties of attention."""
        if device.type != "cuda":
            pytest.skip("FlashAttention requires CUDA")

        try:
            import vllm.vllm_flash_attn

            assert hasattr(vllm.vllm_flash_attn, "flash_attn_varlen_func")
        except (ImportError, AttributeError):
            pytest.skip("FlashAttention package not available")

        impl = FlashAttentionImpl(attn_type=AttentionType.ENCODER,
                                  **attention_params)

        query, key, value = self.create_realistic_tensors(device)
        metadata = self.create_simple_metadata(device,
                                               attn_type=AttentionType.ENCODER)

        class MockLayer:
            _k_scale = torch.tensor(1.0, device=device)
            _v_scale = torch.tensor(1.0, device=device)
            _q_scale = torch.tensor(1.0, device=device)

        layer = MockLayer()
        output = torch.zeros_like(query)
        kv_cache = torch.zeros((2, 1, 16, 2, 32),
                               device=device,
                               dtype=torch.float16)

        result = impl.forward(layer, query, key, value, kv_cache, metadata,
                              output)

        # Test basic properties
        assert result.shape == query.shape, (
            "Output shape should match query shape")
        assert result.dtype == query.dtype, (
            "Output dtype should match query dtype")
        assert result.device == query.device, (
            "Output device should match query device")
        assert torch.isfinite(result).all(), "All outputs should be finite"

        # Test that zero values produce zero outputs (when appropriate)
        zero_value = torch.zeros_like(value)
        zero_output = torch.zeros_like(query)
        zero_result = impl.forward(layer, query, key, zero_value, kv_cache,
                                   metadata, zero_output)
        assert torch.allclose(
            zero_result, torch.zeros_like(zero_result),
            atol=1e-6), "Zero values should produce zero outputs"

        # Test scaling property: if all values are scaled by a factor,
        # output should be scaled by same factor
        scale_factor = 2.0
        scaled_value = value * scale_factor
        scaled_output = torch.zeros_like(query)
        scaled_result = impl.forward(layer, query, key, scaled_value, kv_cache,
                                     metadata, scaled_output)

        expected_scaled = result * scale_factor
        assert torch.allclose(
            scaled_result, expected_scaled,
            atol=1e-5), ("Scaling values should scale output proportionally")

        print("✅ Attention satisfies fundamental mathematical properties")

    def test_cross_attention_metadata_validation(self, device,
                                                 attention_params):
        """Test that cross-attention works with proper metadata."""
        if device.type != "cuda":
            pytest.skip("FlashAttention requires CUDA")

        try:
            import vllm.vllm_flash_attn

            assert hasattr(vllm.vllm_flash_attn, "flash_attn_varlen_func")
        except (ImportError, AttributeError):
            pytest.skip("FlashAttention package not available")

        impl = FlashAttentionImpl(attn_type=AttentionType.ENCODER_DECODER,
                                  **attention_params)

        # Create decoder queries and encoder keys/values
        query, _, _ = self.create_realistic_tensors(
            device, seq_len=3)  # decoder tokens
        _, encoder_key, encoder_value = self.create_realistic_tensors(
            device, seq_len=2, seed=123)  # encoder tokens

        metadata = self.create_simple_metadata(
            device, attn_type=AttentionType.ENCODER_DECODER)

        class MockLayer:
            _k_scale = torch.tensor(1.0, device=device)
            _v_scale = torch.tensor(1.0, device=device)
            _q_scale = torch.tensor(1.0, device=device)

        layer = MockLayer()
        output = torch.zeros_like(query)

        # For cross-attention, use empty KV cache to force direct tensor path
        # But ensure the cache has proper structure
        kv_cache = torch.zeros((2, 1, 16, 2, 32),
                               device=device,
                               dtype=torch.float16)

        # This should work without crashing when metadata is properly set
        result = impl.forward(layer, query, encoder_key, encoder_value,
                              kv_cache, metadata, output)

        # Basic checks
        assert result.shape == query.shape, (
            "Cross-attention output shape should match query shape")
        assert torch.isfinite(result).all(), (
            "Cross-attention output should be finite")
        assert not torch.allclose(result, torch.zeros_like(result)), (
            "Cross-attention should produce non-zero output")

        print("✅ Cross-attention works correctly with proper metadata")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
